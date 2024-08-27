package client

import (
	"context"

	"github.com/francescopepe/formigo/internal/messages"
)

type MessageReceiver interface {
	ReceiveMessages(ctx context.Context) ([]messages.Message, error)
}

type MessageDeleter interface {
	DeleteMessages(messages []messages.Message) error
}

type Client interface {
	MessageReceiver
	MessageDeleter
}
