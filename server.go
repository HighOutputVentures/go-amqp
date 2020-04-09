package gamqp

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"time"

	"pack.ag/amqp"

	log "github.com/sirupsen/logrus"
)

var typeOfApplicationError = reflect.TypeOf(&(ApplicationError{}))

var unsupportedReplyType = []reflect.Kind{
	reflect.Ptr,
	reflect.Chan,
	reflect.Func,
	reflect.Map,
	reflect.UnsafePointer,
}

type methodType struct {
	sync.Mutex

	name     string
	typ      reflect.Type
	receiver reflect.Value
	method   reflect.Method
}

// Server is the interface implemented for all servers
type Server interface {
	Service
	RegisterName(string, interface{})
	Register(interface{})
}

type service struct {
	name     string // name of the service
	receiver reflect.Value
	methods  map[string]*methodType
}

// rpcServer the receiver of the messages for RPC
type rpcServer struct {
	sync.Mutex

	concurrency    uint32
	queue          string
	session        *amqp.Session
	timeout        time.Duration
	connection     *Connection
	done           chan bool
	serviceMap     sync.Map
	requestParser  RequestParser
	replyParser    ReplyParser
	methods        map[string]*methodType
	senderCache    *SenderCache
	cancelReceiver context.CancelFunc
	stopping bool
	wg             sync.WaitGroup
}

// New opens a connection and session to the broker
func NewServer(
	connection *Connection,
	queue string,
	concurrency uint32,
	timeout time.Duration,
	requestParser RequestParser,
	replyParser ReplyParser,
) (Server, error) {
	session, err := connection.NewSession()
	if err != nil {
		return nil, err
	}

	senderCache, err := NewSenderCache(connection)
	if err != nil {
		return nil, err
	}

	return &rpcServer{
		connection:    connection,
		session:       session,
		queue:         queue,
		concurrency:   concurrency,
		timeout:       timeout,
		requestParser: requestParser,
		replyParser:   replyParser,
		senderCache:   senderCache,
		done:          make(chan bool),
	}, nil
}

func isTypeSupported(t reflect.Kind) bool {
	for i := 0; i < len(unsupportedReplyType); i++ {
		if t == unsupportedReplyType[i] {
			return false
		}
	}

	return true
}

func suitableMethods(receiver interface{}) map[string]*methodType {
	methods := make(map[string]*methodType)
	typ := reflect.TypeOf(receiver)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		mrcvr := reflect.ValueOf(receiver)
		methods[mname] = &methodType{method: method, typ: mtype, name: mname, receiver: mrcvr}

		if mtype.NumIn() < 2 {
			log.Panicf("method %q needs to have at least 1 argument", mname)
			return nil
		}
		log.Printf("method %q num params %d", mname, mtype.NumIn())
		if mtype.In(1).String() != "context.Context" {
			log.Panicf("method %q needs to have a first parameter that is a %q.", mname, "context.Context")
		}

		if mtype.NumOut() != 2 {
			log.Panicf("method %q needs exactly 2 return value but got %d", mname, mtype.NumOut())
			return nil
		}

		replyType := mtype.Out(0).Kind()
		if !isTypeSupported(replyType) {
			log.Panicf("method %q first return value type is not supported", mname)
		}

		if errType := mtype.Out(1); errType != typeOfApplicationError {
			log.Panicf("method %q second return value must be of type 'ApplicationError'", mname)
		}
	}
	return methods
}

func (w *rpcServer) Stop(ctx context.Context) error {
	w.Lock()
	w.stopping = true
	w.Unlock()

	w.cleanupReceiver()

	c := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *rpcServer) RegisterName(name string, receiver interface{}) {
	s := new(service)

	s.methods = suitableMethods(receiver)
	s.receiver = reflect.ValueOf(receiver)
	s.name = name

	if _, exists := w.serviceMap.LoadOrStore(s.name, s); exists {
		panic("Service is already registered.")
	}
}

func (w *rpcServer) Register(receiver interface{}) {
	name := reflect.Indirect(reflect.ValueOf(receiver)).Type().Name()
	w.RegisterName(name, receiver)
}

func (w *rpcServer) replyTo(address string, message *amqp.Message) {
	if address == "" {
		log.Error("server: replyTo: ", "trying to reply to a non valid address")
		return
	}

	sender, err := w.senderCache.GetRpcSender(address)
	if err != nil {
		log.Errorf("server: replyTo: failed to get sender for %s", address)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	if err = sender.Send(ctx, message); err != nil {
		if err != context.Canceled {
			log.Errorf( "server: replyTo: %s", err)
			return
		}

		log.Errorf("server: replyTo: failed to send message got error, %s", err)
	}
}

func (w *rpcServer) replyToMessage(to *amqp.Message, message *amqp.Message) {
	if message.Properties == nil {
		message.Properties = &amqp.MessageProperties{}
	}

	if to.Properties == nil || to.Properties.ReplyTo == "" {
		log.Error("cannot reply to a message with no ReplyTo property")
		return
	}

	if to.Properties.CorrelationID != "" {
		message.Properties.CorrelationID = to.Properties.CorrelationID
	}

	w.replyTo(to.Properties.ReplyTo, message)
}

func (w *rpcServer) process(message *amqp.Message) {
	defer w.wg.Done()

	body, err := w.requestParser(message)
	if err != nil {
		w.replyToMessage(
			message,
			w.replyParser(nil, NewInvalidRequestError(message.Properties.CorrelationID.(string))),
		)
		return
	}

	dot := strings.LastIndex(body.Method, ".")
	serviceName := ""
	methodName := body.Method

	if dot != -1 {
		serviceName = body.Method[:dot]
		methodName = body.Method[dot+1:]
	}

	found, _ := w.serviceMap.Load(serviceName)
	if found == nil {
		w.replyToMessage(message, w.replyParser(nil, NewServiceNotFoundError(serviceName)))
		return
	}

	svc := found.(*service)
	method := svc.methods[methodName]

	if method == nil {
		w.replyToMessage(message, w.replyParser(nil, NewMethodNotFoundError(methodName)))
		return
	}

	param := reflect.New(method.typ.In(2))
	if err := json.Unmarshal(body.Parameters, param.Interface()); err != nil {
		w.replyToMessage(message, w.replyParser(nil, NewInvalidParameterError(serviceName, methodName)))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := method.method.Func.Call([]reflect.Value{method.receiver, reflect.ValueOf(ctx), param.Elem()})
	applicationError := result[1].Interface().(*ApplicationError)
	if applicationError != nil {
		w.replyToMessage(message, w.replyParser(nil, applicationError))
		return
	}

	w.replyToMessage(message, w.replyParser(result[0].Interface(), nil))
	return
}

func (w *rpcServer) cleanupReceiver() {
	if w.cancelReceiver == nil {
		return
	}

	w.Lock()
	defer w.Unlock()

	w.cancelReceiver()
	w.cancelReceiver = nil
}

func (w *rpcServer) receiveMessages(receiver *amqp.Receiver, messages chan<- *amqp.Message) {
	for !w.stopping {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			w.Lock()
			w.cancelReceiver = cancel
			w.Unlock()
			defer w.cleanupReceiver()

			msg, err := receiver.Receive(ctx)
			if err != nil {
				if err != context.Canceled {
					log.Error("server: receiveMessages: ", err)
				}
				return
			}

			messages <- msg
			err = msg.Accept()
			if err != nil {
				log.Errorf("server: start: accept: failed to accept the message, got error %s", err)
			}
		}()
	}
}

// Start accepting messages from the clients
func (w *rpcServer) Start() error {
	receiver, err := w.session.NewReceiver(
		amqp.LinkSourceAddress(queueAddress(w.queue)),
		amqp.LinkCredit(w.concurrency),
		amqp.LinkSourceDurability(amqp.DurabilityUnsettledState),
		amqp.LinkSourceExpiryPolicy(amqp.ExpiryNever),
	)

	if err != nil {
		return err
	}

	w.stopping = false
	messages := make(chan *amqp.Message)

	go w.receiveMessages(receiver, messages)
	go w.processIncomingMessage(messages)

	return nil
}

func (w *rpcServer) processIncomingMessage(messages chan *amqp.Message) {
	for !w.stopping {
		message := <-messages

		w.wg.Add(1)
		w.process(message)

		if err := message.Accept(); err != nil {
			log.Fatalf("server: processIncomingMessage: failed to accept the message, got error %s", err)
		}
	}
}
