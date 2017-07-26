package message

import (
	"github.com/ONSdigital/dp-dimension-importer/kafka"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/log"
)

type EventHandler interface {
	HandleEvent(event model.DimensionsExtractedEvent)
}

func Consume(incoming chan kafka.Message, eventHandler EventHandler) {
	for msg := range incoming {
		var event model.DimensionsExtractedEvent
		err := schema.DimensionsExtractedSchema.Unmarshal(msg.GetData(), &event)

		if err != nil {
			panic(err)
		}

		log.Debug("Recieved DimensionsExtractedEvent", log.Data{
			"Event": event,
		})

		eventHandler.HandleEvent(event)
		log.Debug("instance has been imported", log.Data{
			"instance_id": event.InstanceID,
		})
	}
}
