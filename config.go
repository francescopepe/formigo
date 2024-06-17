package formigo

import (
	"log"
	"time"

	"github.com/francescopepe/formigo/internal/client"
)

const (
	defaultErrorThreshold       = 3
	defaultErrorPeriod          = time.Second * 120
	defaultConcurrency          = 100
	defaultRetrievers           = 1
	defaultDeleterBufferSize    = 10
	defaultDeleterBufferTimeout = time.Millisecond * 500
)

type DeleterConfiguration struct {
	BufferSize    int
	BufferTimeout time.Duration
}

// The ErrorConfiguration defines a threshold for which the worker stops. If the number
// of errors occurred during the worker execution passes the given Threshold on the
// specified Period, the worker stops.
type ErrorConfiguration struct {
	// Number of errors that must occur in the Period before the worker stops.
	// Default: 3.
	Threshold int

	// Duration of the period for which, if the number of errors passes the Threshold, the worker stops.
	// Default: 120s.
	Period time.Duration

	// The error report function, returns a boolean value to decide whether the error counts towards to threshold
	ReportFunc func(err error) bool
}

// The MultiMessageBufferConfiguration defines a buffer which is consumed by the worker when either
// the buffer is full or the timeout has passed since the first message got added.
type MultiMessageBufferConfiguration struct {
	// Max number of messages that the buffer can contain.
	// Default: 10.
	Size int

	// Time after which the buffer gets processed, no matter whether it is full or not.
	// This value MUST be smaller tha VisibilityTimeout in the
	// RetrieveMessageConfiguration + the maximum processing time of the handler.
	// If this is not set correctly, the same message could be processed multiple times.
	// Default: 1s.
	Timeout time.Duration
}

type SingleMessageConsumerConfiguration struct {
	Handler singleMessageHandler
}

type MultiMessageConsumerConfiguration struct {
	Handler      multiMessageHandler
	BufferConfig MultiMessageBufferConfiguration
}

type Configuration struct {
	// A queue client
	Client client.Client

	// Number of Go routines that process the messages from the Queue.
	// The higher this value, the more Go routines are spawned to process the messages.
	// Using a high value can be useful when the Handler of the consumer perform slow I/O operations.
	// Default: 100.
	Concurrency int

	// Number of Go routines that retrieve messages from the Queue.
	// The higher this value, the more Go routines are spawned to read the messages from the
	// queue and provide them to the worker's consumers.
	// Using a high value can be useful when the network is slow or when consumers are quicker
	// than retrievers.
	// Default: 1.
	Retrievers int

	// The ErrorConfiguration.
	ErrorConfig ErrorConfiguration

	// The messages Consumer.
	Consumer consumer

	// Configuration for the deleter
	DeleterConfig DeleterConfiguration
}

func setWorkerConfigValues(config Configuration) Configuration {
	if config.Retrievers == 0 {
		config.Retrievers = defaultRetrievers
	}

	if config.Concurrency == 0 {
		config.Concurrency = defaultConcurrency
	}

	if config.ErrorConfig.Threshold == 0 {
		config.ErrorConfig.Threshold = defaultErrorThreshold
	}

	if config.ErrorConfig.Period == 0 {
		config.ErrorConfig.Period = defaultErrorPeriod
	}

	if config.ErrorConfig.ReportFunc == nil {
		config.ErrorConfig.ReportFunc = func(err error) bool {
			log.Println("ERROR", err)

			return true
		}
	}

	if config.DeleterConfig.BufferSize == 0 {
		config.DeleterConfig.BufferSize = defaultDeleterBufferSize
	}

	if config.DeleterConfig.BufferTimeout == 0 {
		config.DeleterConfig.BufferTimeout = defaultDeleterBufferTimeout
	}

	return config
}
