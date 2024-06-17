package formigo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/francescopepe/formigo/internal/messages"
)

type singleMessageHandler = func(ctx context.Context, msg interface{}) error
type multiMessageHandler = func(ctx context.Context, msgs []interface{}) error

// This means that the buffered messages didn't get passed to the handler within
// the first message's timeout.
// This is generally due to:
// - Visibility timeout of the messages too small
// - Buffer timeout too high
// - Consumer to slow
var errBufferCtxExpired = errors.New("buffer context expired, buffer will Reset")

type consumer interface {
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

// singleMessageConsumer defines a message handler that consumes only one message at a
// time.
// It can be useful when the workload is specific per message, for example for sending
// an email.
type singleMessageConsumer struct {
	handler singleMessageHandler
}

func (c *singleMessageConsumer) processMessage(msg messages.Message) error {
	defer msg.CancelCtx() // This must be called to release resources associated with the context.

	// Process Message
	return wrapHandler(func() error {
		return c.handler(msg.Ctx, msg.Msg)
	})
}

// Consumes and deletes a single message, it stops only when the `messageCh` gets closed
// and doesn't have any messages in it.
func (c *singleMessageConsumer) consume(concurrency int, ctrl *controller, messageCh <-chan messages.Message, deleteCh chan<- messages.Message) {
	consumers := makeAvailableConsumers(concurrency)

	var wg sync.WaitGroup
	for msg := range messageCh {
		<-consumers // Use an available comsumer

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

func NewSingleMessageConsumer(config SingleMessageConsumerConfiguration) *singleMessageConsumer {
	return &singleMessageConsumer{
		handler: config.Handler,
	}
}

// multiMessageConsumer allows to process multiple messages at a time. This can be useful
// for batch updates or use cases with high throughput.
type multiMessageConsumer struct {
	handler      multiMessageHandler
	bufferConfig MultiMessageBufferConfiguration
}

// It processes the messages and push them downstream for deletion.
func (c *multiMessageConsumer) processMessages(ctrl *controller, deleteCh chan<- messages.Message, ctx context.Context, messages []messages.Message) {
	msgs := make([]interface{}, 0, len(messages))
	for _, msg := range messages {
		msgs = append(msgs, msg.Msg)
	}
	err := wrapHandler(func() error {
		return c.handler(ctx, msgs)
	})
	if err != nil {
		ctrl.reportError(fmt.Errorf("failed to process messages: %w", err))
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
func (c *multiMessageConsumer) consume(concurrency int, ctrl *controller, messageCh <-chan messages.Message, deleteCh chan<- messages.Message) {
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

func NewMultiMessageConsumer(config MultiMessageConsumerConfiguration) *multiMessageConsumer {
	if config.BufferConfig.Size == 0 {
		config.BufferConfig.Size = 10
	}

	if config.BufferConfig.Timeout == 0 {
		config.BufferConfig.Timeout = time.Second
	}

	return &multiMessageConsumer{
		handler:      config.Handler,
		bufferConfig: config.BufferConfig,
	}
}

// Interface guards
var (
	_ consumer = (*singleMessageConsumer)(nil)
	_ consumer = (*multiMessageConsumer)(nil)
)
