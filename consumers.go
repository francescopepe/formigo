package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/francescopepe/go-queue-worker/internal/messages"
)

type SingleMessageHandler = func(ctx context.Context, msg interface{}) error
type MultiMessageHandler = func(ctx context.Context, msgs []interface{}) error

type consumer interface {
	consume(errorCh chan<- error, messageCh <-chan messages.Message, deleteCh chan<- messages.Message)
}

// wrapHandler catches any panic error, logs it and returns the error that generated it.
// It prevents the worker from crashing in case of an unexpected error.
func wrapHandler(handler func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Set error before returning
			err = fmt.Errorf("panic error: %s", r)
		}
	}()

	err = handler()

	return err
}

// SingleMessageConsumer defines a message handler that consumes only one message at a
// time.
// It can be useful when the workload is specific per message, for example for sending
// an email.
type SingleMessageConsumer struct {
	handler SingleMessageHandler
}

func (c *SingleMessageConsumer) processMessage(errorCh chan<- error, msg messages.Message) error {
	defer msg.CancelCtx() // This must be called to release resources associated with the context.

	// Process Message
	return wrapHandler(func() error {
		return c.handler(msg.Ctx, msg.Msg)
	})
}

// Consumes and deletes a single message, it stops only when the `messageCh` gets closed
// and doesn't have any messages in it.
func (c *SingleMessageConsumer) consume(errorCh chan<- error, messageCh <-chan messages.Message, deleteCh chan<- messages.Message) {
	// For each message, until closure
	for msg := range messageCh {
		err := c.processMessage(errorCh, msg)
		if err != nil {
			errorCh <- fmt.Errorf("failed to process message: %w", err)
			continue
		}

		// Push message for deletion
		deleteCh <- msg
	}
}

func NewSingleMessageConsumer(config SingleMessageConsumerConfiguration) *SingleMessageConsumer {
	return &SingleMessageConsumer{
		handler: config.Handler,
	}
}

// MultiMessageConsumer allows to process multiple messages at a time. This can be useful
// for batch updates or use cases with high throughput.
type MultiMessageConsumer struct {
	handler      MultiMessageHandler
	bufferConfig MultiMessageBufferConfiguration
}

// It processes the messages and push them downstream for deletion.
func (c *MultiMessageConsumer) processMessages(errorCh chan<- error, deleteCh chan<- messages.Message, messages []messages.Message, ctx context.Context) {
	msgs := make([]interface{}, 0, len(messages))
	for _, msg := range messages {
		msgs = append(msgs, msg.Msg)
	}
	err := wrapHandler(func() error {
		return c.handler(ctx, msgs)
	})
	if err != nil {
		errorCh <- fmt.Errorf("failed to process messages: %w", err)
		return
	}

	// Push messages for deletion
	for _, msg := range messages {
		deleteCh <- msg
	}
}

// Consumes and deletes a number of messages in the interval [1, N] based on configuration
// provided in the BufferConfiguration.
// It stops only when the messageCh gets closed and doesn't have any messages in it.
// This function uses two timeouts:
//   - consumption timeout, set in the context
//   - buffer timeout, used with the timeoutCh
func (c *MultiMessageConsumer) consume(errorCh chan<- error, messageCh <-chan messages.Message, deleteCh chan<- messages.Message) {
	// Create buffer
	buffer := messages.NewBufferWithContextTimeout(messages.BufferWithContextTimeoutConfiguration{
		Size:          c.bufferConfig.Size,
		BufferTimeout: c.bufferConfig.Timeout,
	})
	defer buffer.Reset()

	for {
		select {
		case msg, open := <-messageCh:
			if !open {
				// Message channel closed. This is the stop signal.
				// Note: We can't return if the buffer contains messages to process.
				// We MUST consume all the messages on the buffer
				if buffer.IsEmpty() {
					return // Buffer empty, we can stop
				}
				break // Buffer contains messages, break the select
			}

			// Add message to the buffer
			buffer.Add(msg)

			// If the buffer is not full, continue
			if !buffer.IsFull() {
				continue
			}

		case <-buffer.CtxExpired():
			// Context expired, Reset the buffer and raise an error.
			// This means that the buffered messages didn't get passed to the handler within
			// the first message's timeout.
			// This is generally due to:
			// - Visibility timeout of the messages too small
			// - Buffer timeout too high
			errorCh <- errors.New("buffer context timeout reached, buffer will Reset")

			// Reset the buffer.
			buffer.Reset()
			continue

		case <-buffer.Expired():
			// Timeout expired, process the buffer
		}

		// Process the messages
		c.processMessages(errorCh, deleteCh, buffer.Messages(), buffer.Context())

		// Reset buffer
		buffer.Reset()
	}
}

func NewMultiMessageConsumer(config MultiMessageConsumerConfiguration) *MultiMessageConsumer {
	if config.BufferConfig.Size == 0 {
		config.BufferConfig.Size = 10
	}

	if config.BufferConfig.Timeout == 0 {
		config.BufferConfig.Timeout = time.Second
	}

	return &MultiMessageConsumer{
		handler:      config.Handler,
		bufferConfig: config.BufferConfig,
	}
}

// Interface guards
var (
	_ consumer = (*SingleMessageConsumer)(nil)
	_ consumer = (*MultiMessageConsumer)(nil)
)
