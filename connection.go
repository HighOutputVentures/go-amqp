package gamqp

import (
	"context"
	"pack.ag/amqp"
	"sync"
)

type Connection struct {
	mux      sync.Mutex
	client   *amqp.Client
	sessions []*amqp.Session
}

func NewConnection(address string, username string, password string) (*Connection, error) {
	client, err := amqp.Dial(address, amqp.ConnSASLPlain(username, password))
	return &Connection{client: client}, err
}

func (c *Connection) Close(ctx context.Context) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	for _, session := range c.sessions {
		err := session.Close(ctx)
		if err == amqp.ErrSessionClosed {
			return nil
		}

		if err != nil {
			return err
		}
	}

	return c.client.Close()
}

func (c *Connection) NewSession() (*amqp.Session, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	session, err := c.client.NewSession()
	if err != nil {
		return nil, err
	}

	c.sessions = append(c.sessions, session)
	return session, nil
}
