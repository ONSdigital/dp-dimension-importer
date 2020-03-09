package message

import (
	"context"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/log.go/log"
)

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
	ctx := context.Background()
	logData := log.Data{"package": "message.KafkaMessageReciever"}
	if err := schema.NewInstanceSchema.Unmarshal(message.GetData(), &newInstanceEvent); err != nil {
		log.Event(ctx, "error while attempting to unmarshal kafka message into event.NewInstance", log.ERROR, log.Error(err), logData)
		return
	}

	logData["event"] = newInstanceEvent
	log.Event(ctx, "successfully unmarshalled kafka message into event.NewInstance", log.INFO, logData)

	if err := r.InstanceHandler.Handle(newInstanceEvent); err != nil {
		log.Event(ctx, "InstanceHandler.Handle returned an error", log.ERROR, log.Error(err), logData)
		if err := r.ErrorReporter.Notify(newInstanceEvent.InstanceID, "InstanceHandler.Handle returned an unexpected error", err); err != nil {
			log.Event(ctx, "ErrorReporter.Notify returned an error", log.ERROR, log.Error(err), logData)
		}
		return
	}

	log.Event(ctx, "newInstance event successfully processed", log.INFO, logData)
	message.Commit()
}
