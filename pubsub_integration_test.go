package gamqp_test

import (
	gamqp "HighOutputVentures/go-amqp"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"pack.ag/amqp"
)

func createSubscriber(t *testing.T, wg *sync.WaitGroup, ctx context.Context, connection *gamqp.Connection, times int) {
	defer wg.Done()

	received := 0
	sub := gamqp.NewSubscriber(connection, "test", 1)

	go func() {
		if err := sub.Start(); err != nil {
			t.Fatalf("expected to successfully start the subscriber")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("expected to not reach timeout")
		case <-sub.Messages():
			received++
		}

		if received == times {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := sub.Stop(stopCtx); err != nil {
				t.Fatalf("expected to successfully stop the subscriber but got, %q", err)
			}
			cancel()
			break
		}
	}
}

func assertConnection(t *testing.T) *gamqp.Connection {
	connection, err := gamqp.NewConnection("amqp://localhost:5672", "anonymous", "secret")
	if err != nil {
		t.Fatal("expecting to successfully connect")
	}

	return connection
}

// TODO: Add test for multiple subscriber - ensure that all subscriber received the message
func TestPubSub(t *testing.T) {
	t.Log("connecting...")
	connection := assertConnection(t)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	messageTimes := 10
	wg := sync.WaitGroup{}
	wg.Add(3)
	go createSubscriber(t, &wg, ctx, connection, messageTimes)
	go createSubscriber(t, &wg, ctx, connection, messageTimes)
	go createSubscriber(t, &wg, ctx, connection, messageTimes)

	time.Sleep(2 * time.Second)

	connection = assertConnection(t)
	pub := gamqp.NewPublisher(connection, "test")

	go func() {
		if err := pub.Start(); err != nil {
			t.Fatalf("expected to not have an error but got %v", err)
			return
		}

		for i := 0; i < messageTimes; i++ {
			err := pub.Publish(context.Background(), amqp.NewMessage([]byte(fmt.Sprintf("%d", i))))
			if err != nil {
				t.Fatal("expected to successfully publish")
			}
		}
	}()

	wg.Wait()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pub.Stop(ctx); err != nil {
		t.Fatalf("expected to sucessfully stop the publisher but got %q", err)
		return
	}
}
