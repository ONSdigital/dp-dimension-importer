package message

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
)

var loggerR = logging.Logger{Name: "message.KafkaMessageReciever"}

// InstanceEventHandler handles a event.NewInstance
type InstanceEventHandler interface {
	Handle(e event.NewInstance) error
}

// KafkaMessageReciever is a Receiver for handling incoming kafka messages
type KafkaMessageReciever struct {
	InstanceHandler InstanceEventHandler
	ErrorReporter   reporter.ErrorReporter
}

// OnMessage unmarshal the kafka message and pass it to the InstanceEventHandler any errors are sent to the ErrorReporter
func (r KafkaMessageReciever) OnMessage(message kafka.Message) {
	var newInstanceEvent event.NewInstance
	if err := schema.NewInstanceSchema.Unmarshal(message.GetData(), &newInstanceEvent); err != nil {
		loggerR.ErrorC("error while attempting to unmarshal kafka message into event.NewInstance", err, nil)
		return
	}

	logData := map[string]interface{}{"event": newInstanceEvent}
	loggerR.Info("successfully unmarshalled kafka message into event.NewInstance", logData)

	if err := r.InstanceHandler.Handle(newInstanceEvent); err != nil {
		loggerR.ErrorC("InstanceHandler.Handle returned an error", err, logData)
		if err := r.ErrorReporter.Notify(newInstanceEvent.InstanceID, "InstanceHandler.Handle returned an unexpected error", err); err != nil {
			loggerR.ErrorC("ErrorReporter.Notify returned an error", err, logData)
		}
		return
	}

	loggerR.Info("newInstance event successfully processed", logData)
	message.Commit()
}
