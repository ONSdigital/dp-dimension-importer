package handler

import (
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/dp-dimension-importer/event"
)

//go:generate moq -out ../mocks/error_handler_generated_mocks.go -pkg mocks . MessageProducer Marshaller

const (
	errEventType      = "Error"
	avroErr           = "unexpected error marshalling InstanceCompletedSchema to avro bytes. Error not sent to error reporter."
	errEncounteredMsg = "[errorHandler] sending error event to import reporter"
)

type MessageProducer interface {
	Output() chan []byte
}

type Marshaller interface {
	Marshal(s interface{}) ([]byte, error)
}

type ErrorHandler struct {
	Producer   MessageProducer
	Marshaller Marshaller
}

func (e *ErrorHandler) Handle(instanceID string, err error, data log.Data) {
	log.ErrorC(errEncounteredMsg, err, nil)
	errEvent := event.Error{
		InstanceID: instanceID,
		EventType:  errEventType,
		EventMsg:   err.Error(),
	}

	var avroBytes []byte
	if avroBytes, err = e.Marshaller.Marshal(errEvent); err != nil {
		log.ErrorC(avroErr, err, log.Data{"event": errEvent})
		return
	}

	e.Producer.Output() <- avroBytes
}
