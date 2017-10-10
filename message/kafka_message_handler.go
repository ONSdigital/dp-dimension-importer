package message

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

const (
	reporterErr = "unexpected error returned from import error reporter"
)

// InstanceHandler defines an InstanceHandler.
type InstanceEventHandler interface {
	Handle(e event.NewInstance) error
}

// KafkaMessageReciever  ...
type KafkaMessageReciever struct {
	InstanceHandler InstanceEventHandler
	ErrorReporter   reporter.ErrorReporter
}

func (r KafkaMessageReciever) OnMessage(message kafka.Message) {
	var newInstanceEvent event.NewInstance
	if err := schema.NewInstanceSchema.Unmarshal(message.GetData(), &newInstanceEvent); err != nil {
		log.ErrorC(unmarshallErrMsg, err, nil)
		return
	}

	logData := map[string]interface{}{eventKey: newInstanceEvent}
	log.Debug(eventRecieved, logData)

	if err := r.InstanceHandler.Handle(newInstanceEvent); err != nil {
		log.ErrorC(eventHandlerErr, err, logData)
		if err := r.ErrorReporter.Notify(newInstanceEvent.InstanceID, eventHandlerErr, err); err != nil {
			log.Info(reporterErr, logData)
		}
		return
	}

	log.Info(processingSuccessful, logData)
	message.Commit()
}
