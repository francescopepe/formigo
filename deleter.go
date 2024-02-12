package worker

import (
	"fmt"
	"sync"

	"github.com/francescopepe/formigo/internal/client"
	"github.com/francescopepe/formigo/internal/messages"
)

// deleter will delete messages from SQS until the delete channel gets closed.
// Any error will be sent to the error channel.
func deleter(wg *sync.WaitGroup, deleter client.MessageDeleter, config DeleterConfiguration, errorCh chan<- error, deleteCh <-chan messages.Message) {
	// Create buffer
	buffer := messages.NewMessageBuffer(messages.BufferConfiguration{
		Size:    config.BufferSize,
		Timeout: config.BufferTimeout,
	})
	defer buffer.Reset()

	for {
		select {
		case msg, open := <-deleteCh:
			if !open {
				// Delete channel closed. This is the stop signal.
				// Note: We can't return if the buffer contains messages to delete.
				// We MUST send the deletion request if the buffer has any messages
				if buffer.IsEmpty() {
					return // Buffer empty, we can stop
				}
				break // Buffer contains messages, break the select
			}

			buffer.Add(msg)

			// If the buffer is not full, continue
			if !buffer.IsFull() {
				continue
			}

		case <-buffer.Expired():
			// Buffer expired, process the buffer
		}

		// Get the messages
		msgs := buffer.Messages()

		// Send deletion request in a separate Go routine to maximise the throughput
		wg.Add(1)
		go func(msgs []messages.Message) {
			defer wg.Done()
			// No context should be passed here. In fact, if the messages were processed correctly
			// we want to do our best to delete it from the queue.
			err := deleter.DeleteMessages(msgs)
			if err != nil {
				errorCh <- fmt.Errorf("unable to delete %d messages: %w", len(msgs), err)
			}
		}(msgs)

		// Reset buffer
		buffer.Reset()
	}
}
