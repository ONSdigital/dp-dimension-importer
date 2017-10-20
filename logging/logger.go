package logging

import (
	"github.com/ONSdigital/go-ns/log"
)

// Logger is a wrapper around GO-NS logging which enables the caller to provide package name which will be added to the
// the log data of each message automatically to give further context to the message
type Logger struct {
	Name string
}

// ErrorC log an error message with a context
func (l Logger) ErrorC(message string, err error, data log.Data) {
	if data == nil {
		data = log.Data{}
	}
	data["package"] = l.Name
	log.ErrorC(message, err, data)
}

// Info log an info message with a prefix if one has been set, if not the message is logged with no prefix
func (l Logger) Info(message string, data log.Data) {
	if data == nil {
		data = log.Data{}
	}
	data["package"] = l.Name
	log.Info(message, data)
}
