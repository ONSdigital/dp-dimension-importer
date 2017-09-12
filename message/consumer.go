package message

import (
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"context"
)

//go:generate moq -out ./message_test/consumer_generated_mocks.go -pkg message_test . KafkaMessage KafkaConsumer CompletedProducer ErrorEventHandler

const (
	eventRecieved        = "recieved NewInstance"
	eventKey             = "event"
	eventHandlerErr      = "unexpected error encountered while handling NewInstance"
	eventHandlerSuccess  = "instance has been successfully imported"
	errorRecieved        = "consumer exit channel recieved error. Exiting dimensionExtractedConsumer"
	conumserErrMsg       = "kafka Consumer Error recieved"
	producerErrMsg       = "completed Error recieved"
	consumerStoppedMsg   = "context.Done invoked. Exiting Consumer loop"
	unmarshallErrMsg     = "unexpected error when unmarshalling avro message to newInstanceEvent"
	processingSuccessful = "instance processed successfully"
	processingErr        = "instance processing failed due to unexpected error"
)

// KafkaMessage type representing a kafka message.
type KafkaMessage kafka.Message

// eventHandler defines an eventHandler.
type EventHandler interface {
	HandleEvent(event event.NewInstance) error
}

type KafkaConsumer interface {
	Incoming() chan kafka.Message
}

// CompletedProducer producer kafka messages for instances that have been successfully processed.
type CompletedProducer interface {
	Completed(e event.InstanceCompleted) error
}

// ErrorEventHandler handler for dealing with any error while processing an inbound message.
type ErrorEventHandler interface {
	Handle(instanceID string, err error, data log.Data)
}

// Consume run a consumer to process incoming messages.
func Consume(ctx context.Context, consumer KafkaConsumer, producer CompletedProducer, eventHandler EventHandler, errorEventHandler ErrorEventHandler) {
	go func() {
		for {
			select {
			case consumedMessage := <-consumer.Incoming():
				processMessage(consumedMessage.GetData(), producer, eventHandler, errorEventHandler)
				consumedMessage.Commit()
			case <-ctx.Done():
				log.Info(consumerStoppedMsg, nil)
				return
			}
		}
	}()
}

func processMessage(consumedData []byte, producer CompletedProducer, eventHandler EventHandler, errorEventHandler ErrorEventHandler) {
	var newInstanceEvent event.NewInstance
	if err := schema.NewInstanceSchema.Unmarshal(consumedData, &newInstanceEvent); err != nil {
		log.ErrorC(unmarshallErrMsg, err, nil)
		errorEventHandler.Handle("", err, nil)
		log.Info(processingErr, nil)
		return
	}

	logData := map[string]interface{}{eventKey: newInstanceEvent}
	log.Debug(eventRecieved, logData)

	if err := eventHandler.HandleEvent(newInstanceEvent); err != nil {
		log.ErrorC(eventHandlerErr, err, logData)
		errorEventHandler.Handle(newInstanceEvent.InstanceID, err, nil)
		log.Info(processingErr, logData)
		return
	}

	logData[logKeys.InstanceID] = newInstanceEvent.InstanceID
	log.Debug(eventHandlerSuccess, logData)

	insertedEvent := event.InstanceCompleted{
		FileURL:    newInstanceEvent.FileURL,
		InstanceID: newInstanceEvent.InstanceID,
	}

	if err := producer.Completed(insertedEvent); err != nil {
		errorEventHandler.Handle(newInstanceEvent.InstanceID, err, nil)
		log.Info(processingErr, logData)
	}

	log.Info(processingSuccessful, logData)
}
