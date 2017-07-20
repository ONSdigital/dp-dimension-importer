package message

import (
	"github.com/ONSdigital/dp-dimension-importer/kafka"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/log"
)

type Handler interface {
	Handle(event model.DimensionsExtractedEvent)
}

type KafkaConsumer interface {
	Consume(incoming chan kafka.Message)
}

type KafkaConsumerImpl struct {
	EventHandler Handler
}

func (k KafkaConsumerImpl) Consume(incoming chan kafka.Message) {
	for msg := range incoming {
		var event model.DimensionsExtractedEvent
		err := schema.DimensionsExtractedSchema.Unmarshal(msg.GetData(), &event)

		if err != nil {
			panic(err)
		}

		log.Debug("Recieved DimensionsExtractedEvent", log.Data{
			"Event": event,
		})

		k.EventHandler.Handle(event)
	}
}
