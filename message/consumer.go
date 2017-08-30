package message

import (
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

//go:generate moq -out ./message_test/consumer_generated_mocks.go -pkg message_test . KafkaMessageConsumer KafkaMessage KafkaMessageProducer InsertedProducer

const (
	eventRecieved       = "Recieved DimensionsExtractedEvent"
	eventKey            = "event"
	eventHandlerErr     = "Unexpected error encountered while handling DimensionsExtractedEvent"
	eventHandlerSuccess = "Instance has been successfully imported"
	errorRecieved       = "Consumer exit channel recieved error. Exiting dimensionExtractedConsumer"
	conumserErrMsg      = "Kafka Consumer Error recieved"
	producerErrMsg      = "InsertedProducer Error recieved"
)

// KafkaMessageConsumer type for consuming kafka messages.
type KafkaMessageConsumer kafka.MessageConsumer

// KafkaMessageProducer type for producing kafka messages.
type KafkaMessageProducer kafka.MessageProducer

// kafkaMessage type representing a kafka message.
type KafkaMessage kafka.Message

// eventHandler defines an eventHandler.
type EventHandler interface {
	HandleEvent(event event.DimensionsExtractedEvent) error
}

// InsertedProducer defines an Producer for dimensions inserted events
type InsertedProducer interface {
	DimensionInserted(e event.DimensionsInsertedEvent) error
	Closer() chan bool
	Errors() chan error
}

// Consume consume incoming kafka messages delegating to the appropriate eventHandler or handling errors.
func Consume(dimensionExtractedConsumer KafkaMessageConsumer, insertedProducer InsertedProducer, eventHandler EventHandler, exitChannel chan error) error {
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

				if err := insertedProducer.DimensionInserted(insertedEvent); err != nil {
					exitChannel <- err
					return
				}
				consumedMessage.Commit()

			case consumerError := <-dimensionExtractedConsumer.Errors():
				log.ErrorC(conumserErrMsg, consumerError, log.Data{logKeys.ErrorDetails: consumerError})
				dimensionExtractedConsumer.Closer() <- true
				insertedProducer.Closer() <- true
				exitChannel <- consumerError
				return
			case producerError := <-insertedProducer.Errors():
				log.ErrorC(producerErrMsg, producerError, nil)
				dimensionExtractedConsumer.Closer() <- true
				insertedProducer.Closer() <- true
				exitChannel <- producerError
				return
			}
		}
	}()
	err := <-exitChannel
	log.ErrorC(errorRecieved, err, nil)
	return err
}
