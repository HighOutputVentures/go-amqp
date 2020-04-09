package gamqp

import (
	"context"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
	"sync"
	"time"
)

type Sender interface {
	Send(*amqp.Message) (*amqp.Message, error)
}

type Client struct {
	Service
	sync.Mutex

	address         string
	receiverAddress string
	sender          *amqp.Sender
	receiver        *amqp.Receiver
	timeout         time.Duration
	requests        map[string]chan *amqp.Message
	wg              sync.WaitGroup

	receiverCancel context.CancelFunc
	stopping       bool
}

func NewClient(connection *Connection, queue string, timeout time.Duration) (*Client, error) {
	session, err := connection.NewSession()
	if err != nil {
		return nil, err
	}

	senderCache, err := NewSenderCache(connection)
	if err != nil {
		return nil, err
	}

	address := queueAddress(queue)
	sender, err := senderCache.GetRpcSender(address)
	if err != nil {
		return nil, err
	}

	receiverAddress := queueAddress(queue + "/" + UUIDV4())
	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress(receiverAddress),
		amqp.LinkCredit(1),
	)

	if err != nil {
		return nil, err
	}

	return &Client{
		address:         address,
		receiverAddress: receiverAddress,
		requests:        make(map[string]chan *amqp.Message),
		sender:          sender,
		receiver:        receiver,
		timeout:         timeout,
		stopping:        false,
	}, nil
}

func (c *Client) Start() error {
	c.Lock()
	c.stopping = false
	c.Unlock()
	for !c.stopping {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			c.Lock()
			c.receiverCancel = cancel
			c.Unlock()

			defer c.cleanupReceiver()

			message, err := c.receiver.Receive(ctx)
			if err != nil {
				if err != context.Canceled {
					log.Error("client: start: receive: ", err)
				}

				return
			}

			err = message.Accept()
			if err != nil {
				log.Warn("client: start: accept: ", err)
				return
			}

			c.Lock()
			defer c.Unlock()
			correlationId := message.Properties.CorrelationID.(string)
			request := c.requests[correlationId]

			if request == nil {
				log.Errorf("client: start: request: unexpected message arrived with correlation id %q", correlationId)
				return
			}

			request <- message
		}()
	}

	return nil
}

func (c *Client) Send(message *amqp.Message) (*amqp.Message, error) {
	if message.Properties == nil {
		message.Properties = &amqp.MessageProperties{}
	}

	correlationId := UUIDV4()
	message.Properties.ReplyTo = c.receiverAddress
	message.Properties.CorrelationID = correlationId

	c.Lock()

	if c.requests[correlationId] != nil {
		c.Unlock()
		return c.Send(message)
	}
	request := make(chan *amqp.Message)
	defer func() {
		c.Lock()
		defer c.Unlock()

		close(request)
		c.requests[correlationId] = nil

	}()

	c.requests[correlationId] = request
	c.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	log.Printf("client: send: sending message to queue %q", message.Properties.ReplyTo)
	err := c.sender.Send(ctx, message)
	if err != nil {
		log.Errorf("client: send: failed to send the message to %s", message.Properties.ReplyTo)
		return nil, err
	}

	c.wg.Add(1)
	defer c.wg.Done()

	select {
	case message := <-request:
		return message, nil
	case <-time.After(c.timeout):
		return nil, ErrTimeout
	}
}

func (c *Client) cleanupReceiver() {
	if c.receiverCancel == nil {
		return
	}
	c.Lock()
	defer c.Unlock()

	c.receiverCancel()
	c.receiverCancel = nil
}

func (c *Client) Stop(ctx context.Context) error {
	c.Lock()
	c.stopping = true
	c.Unlock()

	c.cleanupReceiver()

	ch := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(ch)
	}()

	select {
		case <-ctx.Done():
		return ctx.Err()
		case <-ch:
			return nil
	}
}
