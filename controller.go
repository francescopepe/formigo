package worker

import (
	"context"
	"errors"
	"sync"
	"time"
)

type controller interface {
	run(stopFunc context.CancelCauseFunc, errorCh <-chan error)
}

type defaultController struct {
	errorConfig  ErrorConfiguration
	errorCounter int
	mutex        sync.Mutex
}

// Decrease counter after the timeout period specified in the errorConfig of
// the controller.
func (c *defaultController) decreaseCounterAfterTimeout() {
	time.Sleep(c.errorConfig.Period)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.errorCounter--
}

// Increases the counter and runs a Go routine to decrease the counter after
// the timeout period specified in the errorConfig of the controller.
func (c *defaultController) increaseCounter() {
	// Increase counter
	c.mutex.Lock()
	c.errorCounter++
	c.mutex.Unlock()

	go c.decreaseCounterAfterTimeout()
}

func (c *defaultController) shouldStop() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.errorCounter > c.errorConfig.Threshold
}

func (c *defaultController) run(stopFunc context.CancelCauseFunc, errorCh <-chan error) {
	var once sync.Once

	for {
		select {
		case err, open := <-errorCh:
			if !open {
				return // Stop signal received
			}

			c.errorConfig.Reporter.Report(err)
			c.increaseCounter()

			if c.shouldStop() {
				once.Do(func() {
					stopFunc(errors.New("too many errors within the given period"))
				})
			}
		}
	}
}

func newDefaultController(errorConfig ErrorConfiguration) *defaultController {
	return &defaultController{
		errorConfig: errorConfig,
	}
}

// Interface guards
var (
	_ controller = (*defaultController)(nil)
)
