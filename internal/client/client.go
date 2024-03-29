package client

import "github.com/francescopepe/formigo/internal/messages"

type MessageReceiver interface {
	ReceiveMessages() ([]messages.Message, error)
}

type MessageDeleter interface {
	DeleteMessages(messages []messages.Message) error
}

type Client interface {
	MessageReceiver
	MessageDeleter
}
