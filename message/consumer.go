package message

import (
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
)

type EventHandler interface {
	HandleEvent(event model.DimensionsExtractedEvent)
}

func Consume(consumer kafka.MessageConsumer, eventHandler EventHandler) error {
	exitChannel := make(chan error)
	go func() {
		for {
			select {
			case consumedMessage := <-consumer.Incoming():
				consumedData := consumedMessage.GetData()
				var event model.DimensionsExtractedEvent
				if err := schema.DimensionsExtractedSchema.Unmarshal(consumedData, &event); err != nil {
					exitChannel <- err
					return
				}

				log.Debug("Recieved DimensionsExtractedEvent", log.Data{
					"Event": event,
					// "messageString": string(consumedData),
					// "messageRaw":    consumedData,
					// "messageLen":    len(consumedData),
				})

				eventHandler.HandleEvent(event)
				log.Debug("instance has been imported", log.Data{
					logKeys.InstanceID: event.InstanceID,
				})
				consumedMessage.Commit()

			case consumerError := <-consumer.Errors():
				log.Error(fmt.Errorf("Aborting"), log.Data{"messageReceived": consumerError})
				consumer.Closer() <- true
				exitChannel <- consumerError
				return
			}
		}
	}()
	return <-exitChannel
}
