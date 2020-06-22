package gamqp_test

import (
	gamqp "github.com/HighOutputVentures/go-amqp"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"pack.ag/amqp"
)

type rpcPayload struct {
	Action string          `json:"type"`
	Data   json.RawMessage `json:"data"`
}

func marshalJSON(p interface{}) []byte {
	encoded, _ := json.Marshal(p)
	return encoded
}

type TestService int

type AddPayload struct {
	X int `json:"x"`
	Y int `json:"y"`
}

func (a *TestService) Add(_ context.Context, data AddPayload) (int, *gamqp.ApplicationError) {
	return data.X + data.Y, nil
}

func (*TestService) Subtract(_ context.Context, data AddPayload) (int, *gamqp.ApplicationError) {
	return data.X - data.Y, nil
}

func (*TestService) StringReturn(_ context.Context, data struct{ Message string }) (string, *gamqp.ApplicationError) {
	return data.Message + " world", nil
}

func (*TestService) ErrorReturn(_ context.Context, _ interface{}) (interface{}, *gamqp.ApplicationError) {
	return nil, &gamqp.ApplicationError{
		Code:    "500",
		Message: "message",
		Meta:    nil,
	}
}

func (*TestService) StructReturn(_ context.Context, _ interface{}) (interface{}, *gamqp.ApplicationError) {
	return struct {
		Message string `json:"message"`
	}{Message: "hello world"}, nil
}

func (*TestService) ArrayReturn(_ context.Context, _ interface{}) (interface{}, *gamqp.ApplicationError) {
	return []struct {
		Message string `json:"message"`
	}{{Message: "hello world from array"}}, nil
}

func testIntegrationRequestParser(message *amqp.Message) (*gamqp.Request, error) {
	request := new(rpcPayload)
	if err := json.Unmarshal(message.GetData(), request); err != nil {
		return nil, err
	}

	return &gamqp.Request{
		Method:     request.Action,
		Parameters: request.Data,
	}, nil
}

func testIntegrationReplyParser(data interface{}, appErr *gamqp.ApplicationError) *amqp.Message {
	err := []byte("null")
	result := []byte("null")
	reply := []byte("null")

	if appErr != nil {
		err, _ = json.Marshal(*appErr)
	}

	result, _ = json.Marshal(data)
	reply, _ = json.Marshal(gamqp.Response{
		Error:  err,
		Result: result,
	})

	return amqp.NewMessage(reply)
}

func createServer(t *testing.T, queue string) gamqp.Server {
	connection := assertConnection(t)
	server, err := gamqp.NewServer(
		connection,
		queue,
		1,
		30*time.Second,
		testIntegrationRequestParser,
		testIntegrationReplyParser,
	)

	if err != nil {
		t.Fatalf("expecting to be successfully initialize the server, but got %v", err)
	}

	server.Register(new(TestService))

	go func() {
		if err := server.Start(); err != nil {
			t.Fatalf("expecting to be able to start the server successfully but got %q", err)
			return
		}
		t.Logf("server %q started", queue)
	}()

	return server
}

func createClient(t *testing.T, address string) *gamqp.Client {
	connection := assertConnection(t)
	client, err := gamqp.NewClient(connection, address, 10*time.Second)
	if err != nil {
		t.Fatalf("expecting to successfully create a Client but got %q", err)
	}

	go func() {
		if err := client.Start(); err != nil {
			t.Fatalf("expecting to be able to start the Client successfully but got %q", err)
			return
		}
		t.Logf("Client %q started", address)
	}()
	time.Sleep(1 * time.Second)
	return client
}

func TestRPC(t *testing.T) {
	queue := fmt.Sprintf("%d", 1000000+rand.Intn(10000))
	server := createServer(t, queue)
	client := createClient(t, queue)

	t.Log("sending message")
	response, err := client.Send(amqp.NewMessage(marshalJSON(rpcPayload{
		Action: "TestService.Add",
		Data: marshalJSON(AddPayload{
			X: 1,
			Y: 2,
		}),
	})))

	if err != nil {
		log.Fatalf("expecting to have no error on add operation but got %q", err)
		return
	}

	payload := assertResponse(t, response)
	result := json.Number(payload.Result).String()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if result != "3" {
		t.Fatalf("expected to have a sum of 3 but got %q", result)
	}

	if err := server.Stop(ctx); err != nil {
		t.Fatalf("expecting server to stopping successfully but got %q", err)
		return
	}

	if err := client.Stop(ctx); err != nil {
		t.Fatalf("expecting Client to stopping successfully but got %q", err)
		return
	}
}

func assertResponse(t *testing.T, response *amqp.Message) *gamqp.Response {
	responsePayload := new(gamqp.Response)
	if err := json.Unmarshal(response.GetData(), responsePayload); err != nil {
		t.Fatalf("expecting to be successfully parsed the ")
	}

	return responsePayload
}
