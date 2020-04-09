package gamqp

import (
	"context"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
	"sync"
)

type Subscriber struct {
	Service
	sync.Mutex

	topic          string
	connection     *Connection
	concurrency    uint32
	receiver       *amqp.Receiver
	messages       chan *amqp.Message
	stopping       bool
	receiverCancel context.CancelFunc
}

func NewSubscriber(connection *Connection, topic string, concurrency uint32) *Subscriber {
	return &Subscriber{
		connection:  connection,
		topic:       topic,
		concurrency: concurrency,
		messages:    make(chan *amqp.Message),
		stopping:    false,
	}
}

func (s *Subscriber) Messages() <-chan *amqp.Message {
	return s.messages
}

func (s *Subscriber) Start() error {
	session, err := s.connection.NewSession()
	if err != nil {
		return err
	}

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress("topic://"+s.topic),
		amqp.LinkCredit(s.concurrency),
	)
	s.receiver = receiver

	if err != nil {
		return err
	}

	s.Lock()
	s.stopping = false
	s.Unlock()

	for !s.stopping {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			s.Lock()
			s.receiverCancel = cancel
			s.Unlock()

			msg, err := receiver.Receive(ctx)
			if err != nil {
				if err != context.Canceled {
					log.Warn("subscriber: start: receive: ", err)
				}
				return
			}

			err = msg.Accept()
			if err != nil {
				log.Warn("subscriber: start: accept:", err)
				return
			}
			s.messages <- msg
		}()
	}

	return nil
}

func (s *Subscriber) cleanupReceiver() {
	if s.receiverCancel == nil {
		return
	}

	s.Lock()
	defer s.Unlock()

	s.receiverCancel()
	s.receiverCancel = nil
}

func (s *Subscriber) Stop(ctx context.Context) error {
	s.Lock()
	s.stopping = true
	s.Unlock()

	s.cleanupReceiver()

	return s.receiver.Close(ctx)
}
