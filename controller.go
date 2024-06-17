package formigo

import (
	"context"
	"errors"
	"sync"
	"time"
)

type controller struct {
	errorConfig  ErrorConfiguration
	errorCounter int
	mutex        sync.Mutex
	stopOnce     sync.Once
	stopFunc     context.CancelCauseFunc
}

// Decrease counter after the timeout period specified in the errorConfig of
// the controller.
func (c *controller) decreaseCounterAfterTimeout() {
	time.Sleep(c.errorConfig.Period)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.errorCounter--
}

// Increases the counter
func (c *controller) increaseCounter() {
	// Increase counter
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.errorCounter++
}

func (c *controller) shouldStop() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.errorCounter > c.errorConfig.Threshold
}

func (c *controller) reportError(err error) {
	if shouldIncreaseCounter := c.errorConfig.ReportFunc(err); !shouldIncreaseCounter {
		return
	}

	c.increaseCounter()
	go c.decreaseCounterAfterTimeout()

	if c.shouldStop() {
		c.stopOnce.Do(func() {
			c.stopFunc(errors.New("too many errors within the given period"))
		})
	}
}

func newController(errorConfig ErrorConfiguration, stopFunc context.CancelCauseFunc) *controller {
	return &controller{
		errorConfig: errorConfig,
		stopFunc:    stopFunc,
	}
}
