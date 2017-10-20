package logging

import (
	"fmt"

	"github.com/ONSdigital/go-ns/log"
)

// Logger is a wrapper around GO-NS logging which enables the caller to provide an prefix to the log messages to give
// further context to the message
type Logger struct {
	Prefix string
}

// ErrorC log an error message with a context
func (l Logger) ErrorC(message string, err error, data log.Data) {
	log.ErrorC(message, err, data)
}

// Info log an info message with a prefix if one has been set, if not the message is logged with no prefix
func (l Logger) Info(message string, data log.Data) {
	if len(l.Prefix) > 0 {
		log.Info(fmt.Sprintf("[%s] %s", l.Prefix, message), data)
	} else {
		log.Info(message, data)
	}
}
