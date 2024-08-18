package messages

import (
	"context"
	"time"
)

type Message struct {
	Ctx          context.Context    `json:"-"` // Exclude from JSON
	CancelCtx    context.CancelFunc `json:"-"` // Exclude from JSON
	MsgId        interface{}        `json:"id"`
	Msg          interface{}        `json:"content"`
	ReceivedTime time.Time          `json:"receivedAt"`
}

func (m Message) Id() interface{} {
	return m.MsgId
}

func (m Message) Content() interface{} {
	return m.Msg
}

func (m Message) ReceivedAt() time.Time {
	return m.ReceivedTime
}

type BufferConfiguration struct {
	Size    int
	Timeout time.Duration
}

// Buffer is used to implement a buffer with a size and timeout.
// When the buffer is full, the `full()` method returns true.
// If the buffer has expired, the `timer` will emit.
// Note that buffer is not thread safe. Remember to use a mutex
// in case it's been used by multiple Go routines.
type Buffer struct {
	messages []Message

	// expiredCh emits when the buffer expires.
	// When the buffer is initialised or reset the channel is set
	// so that it never expires.
	expiredCh <-chan time.Time

	size    int
	timeout time.Duration
	timer   *time.Timer
}

// Len returns the number of Messages in the buffer.
func (b *Buffer) Len() int {
	return len(b.messages)
}

// IsFull Returns true if the buffer is full.
func (b *Buffer) IsFull() bool {
	return len(b.messages) == b.size
}

// Add a message to the buffer and set the timer if the buffer was empty.
func (b *Buffer) Add(msg Message) {
	if len(b.messages) == 0 {
		// Create a new timer and assign its channel to expiredCh
		b.timer = time.NewTimer(b.timeout)
		b.expiredCh = b.timer.C
	}

	b.messages = append(b.messages, msg)
}

// IsEmpty returns true if the buffer is empty.
func (b *Buffer) IsEmpty() bool {
	return len(b.messages) == 0
}

// Messages returns the messages held in the buffer.
func (b *Buffer) Messages() []Message {
	return b.messages
}

// Reset flush the messages contained in the buffer and stop the timer.
// It's important to stop the timer to avoid memory leaks. In fact, the
// GC won't collect the timer until its channel expires.
// NOTE: this function should be always called to clean up any buffer
// created. Used in defer can guarantee that it always run.
func (b *Buffer) Reset() {
	if b.timer != nil {
		// Stop the timer to free its resources
		b.timer.Stop()
		b.timer = nil

		// Reset the expiredCh so that it never expires
		b.expiredCh = make(<-chan time.Time)
	}

	if len(b.messages) > 0 {
		b.messages = make([]Message, 0, b.size)
	}
}

// Expired emits when the buffer expires.
func (b *Buffer) Expired() <-chan time.Time {
	return b.expiredCh
}

func NewMessageBuffer(config BufferConfiguration) *Buffer {
	return &Buffer{
		expiredCh: make(<-chan time.Time),
		messages:  make([]Message, 0, config.Size),
		size:      config.Size,
		timeout:   config.Timeout,
		timer:     nil,
	}
}

type BufferWithContextTimeoutConfiguration struct {
	BufferTimeout time.Duration
	CtxTimeout    time.Duration
	Size          int
}

// BufferWithContextTimeout is used to construct a buffer that has a context timeout
// along with the standard buffer timeout. This is used because the messages have to
// be processed within a certain period and if this doesn't happen, the buffer should
// delete the messages in it and reset.
type BufferWithContextTimeout struct {
	*Buffer
	ctx       context.Context
	cancelCtx context.CancelFunc
}

func (b *BufferWithContextTimeout) Add(msg Message) {
	if len(b.messages) == 0 {
		// Set the context of the buffer to first message's context
		b.ctx = msg.Ctx
	}

	// Override the current cancelCtx in a way that cancels all
	// the previous messages' contexts.
	b.cancelCtx = func(cancel context.CancelFunc) context.CancelFunc {
		return func() {
			cancel()
			msg.CancelCtx()
		}
	}(b.cancelCtx)

	b.Buffer.Add(msg)
}

// Reset resets its internal buffer, cancel the current context created and
// reset any timeout.
// It's important to call this function to avoid memory leaks. In fact, the
// GC won't collect any timer or resources allocated within the context.
// NOTE: this function should be always called to clean up any buffer
// created. Used in defer can guarantee that it always run.
func (b *BufferWithContextTimeout) Reset() {
	b.Buffer.Reset()

	b.cancelCtx()                // Be sure to reset any previous context
	b.ctx = context.Background() // Create a context that doesn't expire
	b.cancelCtx = func() {}
}

func (b *BufferWithContextTimeout) CtxExpired() <-chan struct{} {
	return b.ctx.Done()
}

func (b *BufferWithContextTimeout) Context() context.Context {
	return b.ctx
}

func (b *BufferWithContextTimeout) PullContext() (context.Context, context.CancelFunc) {
	ctx, cancelCtx := b.ctx, b.cancelCtx

	b.ctx = context.Background() // Create a context that doesn't expire
	b.cancelCtx = func() {}

	return ctx, cancelCtx
}

func NewBufferWithContextTimeout(config BufferWithContextTimeoutConfiguration) *BufferWithContextTimeout {
	return &BufferWithContextTimeout{
		Buffer: NewMessageBuffer(BufferConfiguration{
			Size:    config.Size,
			Timeout: config.BufferTimeout,
		}),
		ctx:       context.Background(),
		cancelCtx: func() {}, // Empty cancelCtx
	}
}
