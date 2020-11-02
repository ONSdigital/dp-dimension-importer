package message

import (
	"context"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out mock/instance_event_handler.go -pkg mock . InstanceEventHandler

// InstanceEventHandler handles a event.NewInstance
type InstanceEventHandler interface {
	Handle(ctx context.Context, e event.NewInstance) error
}

// KafkaMessageReceiver is a Receiver for handling incoming kafka messages
type KafkaMessageReceiver struct {
	InstanceHandler InstanceEventHandler
	ErrorReporter   reporter.ErrorReporter
}

// OnMessage unmarshal the kafka message and pass it to the InstanceEventHandler any errors are sent to the ErrorReporter
func (r KafkaMessageReceiver) OnMessage(message kafka.Message) {
	var newInstanceEvent event.NewInstance
	// This context will come from the received kafka message
	ctx := context.Background()
	logData := log.Data{"package": "message.KafkaMessageReceiver"}
	if err := schema.NewInstanceSchema.Unmarshal(message.GetData(), &newInstanceEvent); err != nil {
		log.Event(ctx, "error while attempting to unmarshal kafka message into event new instance", log.ERROR, log.Error(err), logData)
		return
	}

	logData["event"] = newInstanceEvent
	log.Event(ctx, "successfully unmarshalled kafka message into event new instance", log.INFO, logData)

	if err := r.InstanceHandler.Handle(ctx, newInstanceEvent); err != nil {
		log.Event(ctx, "instance handler handle returned an error", log.ERROR, log.Error(err), logData)
		if err := r.ErrorReporter.Notify(newInstanceEvent.InstanceID, "InstanceHandler.Handle returned an unexpected error", err); err != nil {
			log.Event(ctx, "error reporter notify returned an error", log.ERROR, log.Error(err), logData)
		}
		return
	}

	log.Event(ctx, "new instance event successfully processed", log.INFO, logData)
	message.Commit()
}
