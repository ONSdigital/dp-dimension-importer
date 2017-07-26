package message

import (
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

var DimensionsExtractedEventHandler func(event model.DimensionsExtractedEvent)

func Consume(consumer kafka.MessageConsumer) error {
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

				DimensionsExtractedEventHandler(event)
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
