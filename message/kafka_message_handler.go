package message

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

// InstanceHandler defines an InstanceHandler.
type InstanceEventHandler interface {
	Handle(e event.NewInstance) error
}

// ErrorHandler handler for dealing with any error while processing an inbound message.
type ErrorHandler interface {
	Handle(instanceID string, err error, data log.Data)
}

// KafkaMessageHandler  ...
type KafkaMessageHandler struct {
	InstanceHandler InstanceEventHandler
	ErrEventHandler ErrorHandler
}

func (hdlr KafkaMessageHandler) Handle(message kafka.Message) {
	var newInstanceEvent event.NewInstance
	if err := schema.NewInstanceSchema.Unmarshal(message.GetData(), &newInstanceEvent); err != nil {
		log.ErrorC(unmarshallErrMsg, err, nil)
		hdlr.ErrEventHandler.Handle("", err, nil)
		log.Info(processingErr, nil)
		return
	}

	logData := map[string]interface{}{eventKey: newInstanceEvent}
	log.Debug(eventRecieved, logData)

	if err := hdlr.InstanceHandler.Handle(newInstanceEvent); err != nil {
		log.ErrorC(eventHandlerErr, err, logData)
		hdlr.ErrEventHandler.Handle(newInstanceEvent.InstanceID, err, nil)
		log.Info(processingErr, logData)
		return
	}

	log.Info(processingSuccessful, logData)
	message.Commit()
}
