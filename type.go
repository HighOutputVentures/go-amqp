package gamqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"pack.ag/amqp"
)

type Service interface {
	Start() error
	Stop(context.Context) error
}

type Request struct {
	Method     string
	Parameters []byte
}

type RequestParser func(*amqp.Message) (*Request, error)
type ReplyParser func(interface{}, *ApplicationError) *amqp.Message

// Response holds the information of the response of the rpcServer
type Response struct {
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"` // present when there is a problem processing the request
}

var (
	ErrTimeout            = errors.New("request timeout")
	ErrServerCloseTimeout = errors.New("failed to close server, timeout reached")
)

func UUIDV4() string {
	return uuid.NewV4().String()
}

// ApplicationError is return when request has a validation error
type ApplicationError struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Meta    interface{} `json:"meta"`
}

func (e *ApplicationError) Error() string {
	return e.Message
}

func queueAddress(queue string) string {
	return "queue://"+queue
}

func NewMethodNotFoundError(method string) *ApplicationError {
	return &ApplicationError{
		Code:    "500",
		Message: fmt.Sprintf("Method name %q does not exists", method),
		Meta:    nil,
	}
}

func NewServiceNotFoundError(service string) *ApplicationError {
	return &ApplicationError{
		Code:    "500",
		Message: fmt.Sprintf("Service name %q does not exists", service),
		Meta:    nil,
	}
}

func NewInvalidParameterError(service string, method string) *ApplicationError {
	return &ApplicationError{
		Code: "500",
		Message: fmt.Sprintf("Invalid parameter supplied to %s.%s ", service, method),
		Meta: nil,
	}
}

func NewInvalidRequestError(correlationId string) *ApplicationError {
	return &ApplicationError{
		Code:    "500",
		Message: "Invalid Request.",
		Meta:    fmt.Sprintf("{\"correlationId\": %q}", correlationId),
	}
}
