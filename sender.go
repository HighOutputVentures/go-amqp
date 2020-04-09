package gamqp

import (
	"sync"

	"pack.ag/amqp"
)

type SenderCache struct {
	senderMap map[string]*amqp.Sender
	session   *amqp.Session
	mux       sync.Mutex
}

func NewSenderCache(connection *Connection) (*SenderCache, error) {
	session, err := connection.NewSession()
	if err != nil {
		return nil, err
	}

	return &SenderCache{
		senderMap: make(map[string]*amqp.Sender),
		session:   session,
	}, nil
}

func (s *SenderCache) GetRpcSender(address string) (*amqp.Sender, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	sender := s.senderMap[address]
	if sender != nil {
		return sender, nil
	}

	sender, err := s.session.NewSender(
		amqp.LinkTargetAddress(address),
		amqp.LinkTargetDurability(amqp.DurabilityUnsettledState),
		amqp.LinkTargetExpiryPolicy(amqp.ExpiryNever),
	)
	if err != nil {
		return nil, err
	}

	s.senderMap[address] = sender
	return sender, nil
}

func (s *SenderCache) GetPublisher(address string) (*amqp.Sender, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	sender := s.senderMap[address]
	if sender != nil {
		return sender, nil
	}

	sender, err := s.session.NewSender(
		amqp.LinkTargetAddress(address),
	)
	if err != nil {
		return nil, err
	}

	s.senderMap[address] = sender
	return sender, nil
}
