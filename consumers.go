package formigo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/francescopepe/formigo/internal/messages"
)

type BatchResponse struct {
	FailedMessagesId []interface{}
}

type MessageHandler = func(ctx context.Context, msg Message) error
type BatchHandler = func(ctx context.Context, msgs []Message) (BatchResponse, error)

// This means that the buffered messages didn't get passed to the handler within
// the first message's timeout.
// This is generally due to:
// - Visibility timeout of the messages too small
// - Buffer timeout too high
// - Consumer to slow
var errBufferCtxExpired = errors.New("buffer context expired, buffer will Reset")

type Consumer interface {
	consume(concurrency int, ctrl *controller, messageCh <-chan messages.Message, deleteCh chan<- messages.Message)
}

func makeAvailableConsumers(concurrency int) chan struct{} {
	consumers := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		consumers <- struct{}{}
	}

	return consumers
}

// wrapHandler catches any panic error and returns the error that generated it.
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

// messageConsumer defines a message handler that consumes only one message at a
// time.
// It can be useful when the workload is specific per message, for example for sending
// an email.
type messageConsumer struct {
	handler MessageHandler
}

func (c *messageConsumer) processMessage(msg messages.Message) error {
	defer msg.CancelCtx() // This must be called to release resources associated with the context.

	// Process Message
	return wrapHandler(func() error {
		return c.handler(msg.Ctx, msg)
	})
}

// Consumes and deletes a single message, it stops only when the `messageCh` gets closed
// and doesn't have any messages in it.
func (c *messageConsumer) consume(concurrency int, ctrl *controller, messageCh <-chan messages.Message, deleteCh chan<- messages.Message) {
	consumers := makeAvailableConsumers(concurrency)

	var wg sync.WaitGroup
	for msg := range messageCh {
		<-consumers // Use an available consumer

		wg.Add(1)
		go func(message messages.Message) {
			defer func() {
				wg.Done()
				consumers <- struct{}{} // Release consumer
			}()

			err := c.processMessage(message)
			if err != nil {
				ctrl.reportError(fmt.Errorf("failed to process message: %w", err))
				return
			}

			// Push message for deletion
			deleteCh <- message
		}(msg)
	}

	wg.Wait()
}

func NewMessageConsumer(config MessageConsumerConfiguration) *messageConsumer {
	return &messageConsumer{
		handler: config.Handler,
	}
}

// batchConsumer allows to process multiple messages at a time. This can be useful
// for batch updates or use cases with high throughput.
type batchConsumer struct {
	handler      BatchHandler
	bufferConfig BatchConsumerBufferConfiguration
}

// It processes the messages and push them downstream for deletion.
func (c *batchConsumer) processMessages(ctrl *controller, deleteCh chan<- messages.Message, ctx context.Context, msgs []messages.Message) {
	defer func() {
		if r := recover(); r != nil {
			ctrl.reportError(fmt.Errorf("panic error: %s", r))
		}
	}()

	// Convert slice to the abstraction
	converted := make([]Message, 0, len(msgs))
	for _, msg := range msgs {
		converted = append(converted, msg)
	}

	resp, err := c.handler(ctx, converted)
	if err != nil {
		ctrl.reportError(fmt.Errorf("failed to process batch: %w", err))
	}

	toDelete := c.buildMessagesToDeleteFromBatchResponse(msgs, resp)
	// Push messages for deletion
	for _, msg := range toDelete {
		deleteCh <- msg
	}
}

// Consumes and deletes a number of messages in the interval [1, N] based on configuration
// provided in the BufferConfiguration.
// It stops only when the messageCh gets closed and doesn't have any messages in it.
func (c *batchConsumer) consume(concurrency int, ctrl *controller, messageCh <-chan messages.Message, deleteCh chan<- messages.Message) {
	consumers := makeAvailableConsumers(concurrency)

	// Create buffer
	buffer := messages.NewBufferWithContextTimeout(messages.BufferWithContextTimeoutConfiguration{
		Size:          c.bufferConfig.Size,
		BufferTimeout: c.bufferConfig.Timeout,
	})
	defer buffer.Reset()

	var wg sync.WaitGroup
	func() {
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
				ctrl.reportError(errBufferCtxExpired)

				// Reset the buffer.
				buffer.Reset()
				continue

			case <-buffer.Expired():
				// Timeout expired, process the buffer
			}

			select {
			case <-consumers: // Use an available consumer
			case <-buffer.CtxExpired():
				ctrl.reportError(errBufferCtxExpired)

				// Reset the buffer.
				buffer.Reset()
				continue
			}

			ctx, cancelCtx := buffer.PullContext()

			wg.Add(1)
			go func(ctx context.Context, ctxCancelFunc context.CancelFunc, msgs []messages.Message) {
				defer func() {
					wg.Done()
					consumers <- struct{}{} // Release consumer
					ctxCancelFunc()         // Cancel context
				}()

				// Process the messages
				c.processMessages(ctrl, deleteCh, ctx, msgs)
			}(ctx, cancelCtx, buffer.Messages())

			// Reset buffer
			buffer.Reset()
		}
	}()

	wg.Wait()
}

func (c *batchConsumer) buildMessagesToDeleteFromBatchResponse(msgs []messages.Message, resp BatchResponse) []messages.Message {
	if len(resp.FailedMessagesId) == 0 {
		return msgs
	}

	toDelete := make([]messages.Message, 0, len(msgs))

	failedMessagesIdIndexed := make(map[interface{}]struct{}, len(resp.FailedMessagesId))
	for _, id := range resp.FailedMessagesId {
		failedMessagesIdIndexed[id] = struct{}{}
	}

	for _, msg := range msgs {
		if _, ok := failedMessagesIdIndexed[msg.Id()]; !ok {
			toDelete = append(toDelete, msg)
		}
	}

	return toDelete
}

func NewBatchConsumer(config BatchConsumerConfiguration) *batchConsumer {
	if config.BufferConfig.Size == 0 {
		config.BufferConfig.Size = 10
	}

	if config.BufferConfig.Timeout == 0 {
		config.BufferConfig.Timeout = time.Second
	}

	return &batchConsumer{
		handler:      config.Handler,
		bufferConfig: config.BufferConfig,
	}
}

// Interface guards
var (
	_ Consumer = (*messageConsumer)(nil)
	_ Consumer = (*batchConsumer)(nil)
)
