package formigo

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/francescopepe/formigo/internal/messages"
	"github.com/stretchr/testify/assert"
)

type SimpleInMemoryBrokerMessage struct {
	messageId   string
	body        string
	deleteReqCh chan struct{}
	deleteAckCh chan struct{}
	timer       *time.Timer
}

type SimpleInMemoryBroker struct {
	visibilityTimeout time.Duration
	queue             chan *SimpleInMemoryBrokerMessage
	inFlights         chan *SimpleInMemoryBrokerMessage
	expired           chan *SimpleInMemoryBrokerMessage

	statics struct {
		rwMutex          sync.RWMutex
		enqueuedMessages int
		inFlightMessages int
	}
}

func NewSimpleInMemoryBroker(visibilityTimeout time.Duration) *SimpleInMemoryBroker {
	return &SimpleInMemoryBroker{
		visibilityTimeout: visibilityTimeout,
		queue:             make(chan *SimpleInMemoryBrokerMessage, 1000),
		inFlights:         make(chan *SimpleInMemoryBrokerMessage),
		expired:           make(chan *SimpleInMemoryBrokerMessage, 1000),
	}
}

func (b *SimpleInMemoryBroker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-b.inFlights:
			go func(ctx context.Context) {
				select {
				case <-ctx.Done():
					return
				case <-msg.deleteReqCh:
					msg.deleteAckCh <- struct{}{}
				case <-msg.timer.C:
					b.expired <- msg
				}
			}(ctx)
		}
	}
}

func (b *SimpleInMemoryBroker) AddMessages(msgs []*SimpleInMemoryBrokerMessage) {
	for _, msg := range msgs {
		b.queue <- msg
		b.statics.rwMutex.Lock()
		b.statics.enqueuedMessages++
		b.statics.rwMutex.Unlock()
	}
}

func (b *SimpleInMemoryBroker) DeleteMessages(msgs []messages.Message) error {
	requestTimer := time.NewTimer(time.Second * 5)
	defer requestTimer.Stop()

	for _, msg := range msgs {
		brokerMsg := msg.Content().(*SimpleInMemoryBrokerMessage)

		select {
		case <-requestTimer.C:
			return fmt.Errorf("failed to delete message %s: request timeout", brokerMsg.messageId)
		case brokerMsg.deleteReqCh <- struct{}{}:
		}

		if !brokerMsg.timer.Stop() {
			return fmt.Errorf("failed to delete message %s: visibility timeout exipired", brokerMsg.messageId)
		}

		<-brokerMsg.deleteAckCh

		b.statics.rwMutex.Lock()
		b.statics.inFlightMessages--
		b.statics.rwMutex.Unlock()
	}

	return nil
}

func (b *SimpleInMemoryBroker) ReceiveMessages(ctx context.Context) ([]messages.Message, error) {
	var polledMessage *SimpleInMemoryBrokerMessage
	select {
	case polledMessage = <-b.expired:
	default:
		timer := time.NewTimer(time.Millisecond * 500)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, nil
		case polledMessage = <-b.expired:
		case polledMessage = <-b.queue:
		}
	}

	polledMessage.timer = time.NewTimer(b.visibilityTimeout)
	polledMessage.deleteReqCh = make(chan struct{})
	polledMessage.deleteAckCh = make(chan struct{})

	time.After(time.Millisecond * 5)

	msg := messages.Message{
		MsgId:        polledMessage.messageId,
		Msg:          polledMessage,
		ReceivedTime: time.Now(),
	}

	// Set a context with timeout
	msg.Ctx, msg.CancelCtx = context.WithTimeout(context.Background(), b.visibilityTimeout)

	// Move the message to inflight
	b.inFlights <- polledMessage
	b.statics.rwMutex.Lock()
	b.statics.enqueuedMessages--
	b.statics.inFlightMessages++
	b.statics.rwMutex.Unlock()

	return []messages.Message{msg}, nil
}

func (b *SimpleInMemoryBroker) EnqueuedMessages() int {
	b.statics.rwMutex.RLock()
	defer b.statics.rwMutex.RUnlock()
	return b.statics.enqueuedMessages
}

func (b *SimpleInMemoryBroker) InFlightMessages() int {
	b.statics.rwMutex.RLock()
	defer b.statics.rwMutex.RUnlock()
	return b.statics.inFlightMessages
}

func TestWorker(t *testing.T) {
	inMemoryBroker := NewSimpleInMemoryBroker(time.Second * 10)
	go inMemoryBroker.run(context.Background())

	t.Run("can receive a message", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		msgs := []*SimpleInMemoryBrokerMessage{
			{
				messageId: "1",
				body:      "Hello, world!",
			},
		}

		inMemoryBroker.AddMessages(msgs)

		wkr := NewWorker(Configuration{
			Client:      inMemoryBroker,
			Concurrency: 1,
			Retrievers:  1,
			ErrorConfig: ErrorConfiguration{
				ReportFunc: func(err error) bool {
					t.Fatalf("unexpected error: %v", err)
					return true
				},
			},
			Consumer: NewMessageConsumer(MessageConsumerConfiguration{
				Handler: func(ctx context.Context, msg Message) error {
					defer cancel()

					assert.Equal(t, "Hello, world!", msg.Content().(*SimpleInMemoryBrokerMessage).body)

					return nil
				},
			}),
		})

		assert.NoError(t, wkr.Run(ctx))
		assert.Equal(t, 0, inMemoryBroker.EnqueuedMessages())
		assert.Equal(t, 0, inMemoryBroker.InFlightMessages())
	})

	t.Run("can receive a batch of messages", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		msgs := []*SimpleInMemoryBrokerMessage{
			{
				messageId: "1",
				body:      "Hello, world 1!",
			},
			{
				messageId: "2",
				body:      "Hello, world 2!",
			},
			{
				messageId: "3",
				body:      "Hello, world 3!",
			},
		}

		inMemoryBroker.AddMessages(msgs)

		wkr := NewWorker(Configuration{
			Client:      inMemoryBroker,
			Concurrency: 1,
			Retrievers:  1,
			ErrorConfig: ErrorConfiguration{
				ReportFunc: func(err error) bool {
					t.Fatalf("unexpected error: %v", err)
					return true
				},
			},
			Consumer: NewBatchConsumer(BatchConsumerConfiguration{
				BufferConfig: BatchConsumerBufferConfiguration{
					Size:    3,
					Timeout: time.Second,
				},
				Handler: func(ctx context.Context, msgs []Message) (BatchResponse, error) {
					defer cancel()

					if len(msgs) < 3 {
						t.Fatalf("expected 3 messages, got %d", len(msgs))
					}

					assert.Equal(t, "Hello, world 1!", msgs[0].Content().(*SimpleInMemoryBrokerMessage).body)
					assert.Equal(t, "Hello, world 2!", msgs[1].Content().(*SimpleInMemoryBrokerMessage).body)
					assert.Equal(t, "Hello, world 3!", msgs[2].Content().(*SimpleInMemoryBrokerMessage).body)

					return BatchResponse{}, nil
				},
			}),
		})

		assert.NoError(t, wkr.Run(ctx))
		assert.Equal(t, 0, inMemoryBroker.EnqueuedMessages())
		assert.Equal(t, 0, inMemoryBroker.InFlightMessages())
	})
}
