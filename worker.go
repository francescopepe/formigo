package worker

import (
	"context"
	"errors"
	"sync"

	"github.com/francescopepe/go-queue-worker/internal/client"
	"github.com/francescopepe/go-queue-worker/internal/messages"
)

type Worker struct {
	client        client.Client
	retrievers    int
	errorConfig   ErrorConfiguration
	consumer      consumer
	deleterConfig DeleterConfiguration
}

func (w Worker) Run(ctx context.Context) error {
	errorCh := make(chan error)
	defer close(errorCh) // Close errorCh to stop controller

	// Create a new context with a cancel function used to stop the worker from the
	// controller in case too many errors occur.
	ctx, cancel := context.WithCancelCause(ctx)

	// Create and run controller
	ctrl := newDefaultController(w.errorConfig)
	go ctrl.run(cancel, errorCh)

	// Run retrievers and get the message channel
	messageCh := w.runRetrievers(ctx, errorCh)

	// Run consumer and get the deletion channel.
	// Note that the context is not given to them because they will only stop once
	// all the messages in the pipeline have been consumed.
	deleteCh := w.runConsumer(errorCh, messageCh)

	// Run deleter.
	// Note that the context is not given to the deleter because it will only stop once
	// all the consumed messages in the pipeline have been deleted.
	var wg sync.WaitGroup // WaitGroup for the deleter
	deleter(&wg, w.client, w.deleterConfig, errorCh, deleteCh)

	// Wait for context cancellation
	// Note that the context can be canceled by the controller or something outside
	// the worker itself (the context passed to the `Run` function)
	<-ctx.Done()

	// Wait for deleter to exit
	wg.Wait()

	// Get the cause of the cancellation
	// If the context was cancelled by the controller, there must be a cause containing
	// the error, otherwise the context error by default is `context.Canceled`, which
	// means that is what a normal stop request (probably SIGTERM).
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
func (w Worker) runRetrievers(ctx context.Context, errorCh chan<- error) <-chan messages.Message {
	messageCh := make(chan messages.Message)

	var wg sync.WaitGroup
	wg.Add(w.retrievers)
	for i := 0; i < w.retrievers; i++ {
		go func() {
			defer wg.Done()
			retriever(ctx, w.client, errorCh, messageCh)
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
func (w Worker) runConsumer(errorCh chan<- error, messageCh <-chan messages.Message) <-chan messages.Message {
	deleteCh := make(chan messages.Message)

	go func() {
		w.consumer.consume(errorCh, messageCh, deleteCh)

		close(deleteCh)
	}()

	return deleteCh
}

func NewWorker(config WorkerConfiguration) Worker {
	config = setWorkerConfigValues(config)

	return Worker{
		client:        config.Client,
		retrievers:    config.Retrievers,
		errorConfig:   config.ErrorConfig,
		consumer:      config.Consumer,
		deleterConfig: config.DeleterConfig,
	}
}
