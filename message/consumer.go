package message

import (
	"fmt"

	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

// EventHandler defines an EventHandler.
type EventHandler interface {
	HandleEvent(event model.DimensionsExtractedEvent) error
}

// Consume consume incoming kafka messages delegating to the appropriate EventHandler or handling errors.
func Consume(consumer kafka.MessageConsumer, producer kafka.MessageProducer, eventHandler EventHandler) error {
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
				if err := Produce(producer, event.InstanceID, event.FileURL); err != nil {
					exitChannel <- err
					return
				}
				consumedMessage.Commit()

			case consumerError := <-consumer.Errors():
				log.Error(fmt.Errorf("Aborting"), log.Data{"messageReceived": consumerError})
				consumer.Closer() <- true
				producer.Closer() <- true
				exitChannel <- consumerError
				return
			case producerError := <-producer.Errors():
				log.Error(fmt.Errorf("Aborting"), log.Data{"messageReceived": producerError})
				consumer.Closer() <- true
				producer.Closer() <- true
				exitChannel <- producerError
				return
			}
		}
	}()
	return <-exitChannel
}
