package worker

import (
	"context"
	"errors"
	"sync"

	"github.com/francescopepe/formigo/internal/client"
	"github.com/francescopepe/formigo/internal/messages"
)

type worker struct {
	client        client.Client
	concurrency   int
	retrievers    int
	errorConfig   ErrorConfiguration
	consumer      consumer
	deleterConfig DeleterConfiguration
}

func (w worker) Run(ctx context.Context) error {
	// Create a new context with a cancel function used to stop the worker from the
	// controller in case too many errors occur.
	ctx, cancel := context.WithCancelCause(ctx)

	// Create controller
	ctrl := newController(w.errorConfig, cancel)

	// Run retrievers and get the message channel
	messageCh := w.runRetrievers(ctx, ctrl)

	// Run consumer and get the deletion channel.
	// Note that the context is not given to them because they will only stop once
	// all the messages in the pipeline have been consumed.
	deleteCh := w.runConsumer(ctrl, messageCh)

	// Run deleter.
	// Note that the context is not given to the deleter because it will only stop once
	// all the consumed messages in the pipeline have been deleted.
	var wg sync.WaitGroup // WaitGroup for the deleter
	deleter(&wg, w.client, w.deleterConfig, ctrl, deleteCh)

	// Wait for deleter to exit
	wg.Wait()

	// Get the cause of the cancellation
	// If the context was cancelled by the controller, there must be a cause containing
	// the error, otherwise the context error by default is `context.Canceled`, which
	// means that it was a normal stop request (probably SIGTERM).
	err := context.Cause(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}

// runRetrievers will run a number of retrievers (Go routines) based on the worker's
// configuration.
// It returns a channel where the messages will be published and, only when all the
// retrievers have stopped, it will close it to broadcast the signal to stop to the
// consumers.
func (w worker) runRetrievers(ctx context.Context, ctrl *controller) <-chan messages.Message {
	messageCh := make(chan messages.Message)

	var wg sync.WaitGroup
	wg.Add(w.retrievers)
	for i := 0; i < w.retrievers; i++ {
		go func() {
			defer wg.Done()
			retriever(ctx, w.client, ctrl, messageCh)
		}()
	}

	go func() {
		wg.Wait()
		close(messageCh)
	}()

	return messageCh
}

// runConsumer runs the worker's consumer.
// It returns a channel where the messages will be published for deletion and,
// only when the consumer has stopped, it will close it to broadcast the
// signal to stop to the deleter.
func (w worker) runConsumer(ctrl *controller, messageCh <-chan messages.Message) <-chan messages.Message {
	deleteCh := make(chan messages.Message)

	go func() {
		w.consumer.consume(w.concurrency, ctrl, messageCh, deleteCh)

		close(deleteCh)
	}()

	return deleteCh
}

func NewWorker(config Configuration) worker {
	config = setWorkerConfigValues(config)

	return worker{
		client:        config.Client,
		concurrency:   config.Concurrency,
		retrievers:    config.Retrievers,
		errorConfig:   config.ErrorConfig,
		consumer:      config.Consumer,
		deleterConfig: config.DeleterConfig,
	}
}
