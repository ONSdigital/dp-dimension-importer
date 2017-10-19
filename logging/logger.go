package logging

import (
	"fmt"

	"github.com/ONSdigital/go-ns/log"
)

type Logger struct {
	Prefix string
}

func (l Logger) ErrorC(message string, err error, data log.Data) {
	log.ErrorC(message, err, data)
}

func (l Logger) Info(message string, data log.Data) {
	log.Info(fmt.Sprintf("[%s] %s", l.Prefix, message), data)
}
