package message

import (
	"fmt"

	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

//go:generate moq -out ./message_test/consumer_generated_mocks.go -pkg message_test . KafkaMessageConsumer KafkaMessage KafkaMessageProducer

const (
	eventRecieved       = "Recieved DimensionsExtractedEvent"
	eventKey            = "event"
	eventHandlerErr     = "Unexpected error encountered while handling DimensionsExtractedEvent"
	eventHandlerSuccess = "Instance has been successfully imported"
	errorRecieved       = "Consumer exit channel recieved error. Exiting dimensionExtractedConsumer"
)

// KafkaMessageConsumer type for consuming kafka messages.
type KafkaMessageConsumer kafka.MessageConsumer

// KafkaMessageProducer type for producing kafka messages.
type KafkaMessageProducer kafka.MessageProducer

// KafkaMessage type representing a kafka message.
type KafkaMessage kafka.Message

// EventHandler defines an EventHandler.
type EventHandler interface {
	HandleEvent(event event.DimensionsExtractedEvent) error
}

// Consume consume incoming kafka messages delegating to the appropriate EventHandler or handling errors.
func Consume(dimensionExtractedConsumer KafkaMessageConsumer, producer DimensionInsertedProducer, eventHandler EventHandler, exitChannel chan error) error {
	go func() {
		for {
			select {
			case consumedMessage := <-dimensionExtractedConsumer.Incoming():
				consumedData := consumedMessage.GetData()
				var dimensionsExtractedEvent event.DimensionsExtractedEvent
				if err := schema.DimensionsExtractedSchema.Unmarshal(consumedData, &dimensionsExtractedEvent); err != nil {
					exitChannel <- err
					return
				}
				logData := map[string]interface{}{eventKey: dimensionsExtractedEvent}
				log.Debug(eventRecieved, logData)

				if err := eventHandler.HandleEvent(dimensionsExtractedEvent); err != nil {
					log.ErrorC(eventHandlerErr, err, logData)
					exitChannel <- err
					return
				}

				logData[logKeys.InstanceID] = dimensionsExtractedEvent.InstanceID
				log.Debug(eventHandlerSuccess, logData)

				insertedEvent := event.DimensionsInsertedEvent{
					FileURL:    dimensionsExtractedEvent.FileURL,
					InstanceID: dimensionsExtractedEvent.InstanceID,
				}

				if err := producer.DimensionInserted(insertedEvent); err != nil {
					exitChannel <- err
					return
				}
				consumedMessage.Commit()

			case consumerError := <-dimensionExtractedConsumer.Errors():
				log.Error(fmt.Errorf("aborting"), log.Data{"message_received": consumerError})
				dimensionExtractedConsumer.Closer() <- true
				producer.Producer.Closer() <- true
				exitChannel <- consumerError
				return
			case producerError := <-producer.Producer.Errors():
				log.Error(fmt.Errorf("aborting"), log.Data{"message_received": producerError})
				dimensionExtractedConsumer.Closer() <- true
				producer.Producer.Closer() <- true
				exitChannel <- producerError
				return
			}
		}
	}()
	err := <-exitChannel
	log.ErrorC(errorRecieved, err, nil)
	return err
}
