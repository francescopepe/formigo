package worker

import "log"

type ErrorReporter interface {
	Report(err error)
}

type DefaultErrorReporter struct{}

func (r DefaultErrorReporter) Report(err error) {
	log.Println("ERROR", err)
}

func NewDefaultErrorReporter() *DefaultErrorReporter {
	return &DefaultErrorReporter{}
}
