package message

import (
	"context"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/log.go/v2/log"
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
	logData := log.Data{"package": "message.KafkaMessageReceiver"}
	var newInstanceEvent event.NewInstance

	// This context will come from the received kafka message
	ctx := context.Background()

	// unmarshal the event
	if err := schema.NewInstanceSchema.Unmarshal(message.GetData(), &newInstanceEvent); err != nil {
		log.Error(ctx, "error while attempting to unmarshal kafka message into event new instance", err, logData)
		return
	}

	logData["event"] = newInstanceEvent
	log.Info(ctx, "successfully unmarshalled kafka message into event new instance", logData)

	// handle event by the provided handler
	if err := r.InstanceHandler.Handle(ctx, newInstanceEvent); err != nil {
		log.Error(ctx, "instance handler handle returned an error", err, logData)
		if err := r.ErrorReporter.Notify(newInstanceEvent.InstanceID, "InstanceHandler.Handle returned an unexpected error", err); err != nil {
			log.Error(ctx, "error reporter notify returned an error", err, logData)
		}
		return
	}

	log.Info(ctx, "new instance event successfully processed", logData)
}
