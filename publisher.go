package gamqp

import (
	"context"

	"pack.ag/amqp"
)

type Publisher interface {
	Service
	Publish(ctx context.Context, message *amqp.Message) error
}

type publisher struct {
	connection *Connection
	topic   string
	session *amqp.Session
	sender  *amqp.Sender
}

func NewPublisher(connection *Connection, topic string) Publisher {
	return &publisher{connection: connection, topic: topic}
}

func (p *publisher) Start() error {
	session, err := p.connection.NewSession()
	if err != nil {
		return err
	}

	sender, err := session.NewSender(
		amqp.LinkTargetAddress("topic://" + p.topic),
	)

	if err != nil {
		return err
	}

	p.session = session
	p.sender = sender
	return nil
}

func (p *publisher) Publish(ctx context.Context, message *amqp.Message) error {
	return p.sender.Send(ctx, message)
}

func (p *publisher) Stop(ctx context.Context) error {
	return p.sender.Close(ctx)
}
