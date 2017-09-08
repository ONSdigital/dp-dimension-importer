package message

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/go-ns/log"
)

const (
	marshalErr = "Unexpected error while attempting to avro marshall event.InstanceCompletedEvent"
)

//go:generate moq -out ./message_test/producer_generated_mocks.go -pkg message_test . Marshaller KafkaProducer

type KafkaProducer interface {
	Output() chan []byte
}

// Marshaller defines a type for marshalling the requested object into the required format.
type Marshaller interface {
	Marshal(s interface{}) ([]byte, error)
}

// InstanceCompletedProducer type for producing DimensionsInsertedEvents to a kafka topic.
type InstanceCompletedProducer struct {
	Marshaller Marshaller
	Producer   KafkaProducer
}

func NewInstanceCompletedProducer(producer KafkaProducer, marshaller Marshaller) InstanceCompletedProducer {
	return InstanceCompletedProducer{
		Producer:   producer,
		Marshaller: marshaller,
	}
}

// Completed kafka message to complete dimension inserted event
func (p InstanceCompletedProducer) Completed(e event.InstanceCompletedEvent) error {
	bytes, avroError := p.Marshaller.Marshal(e)
	if avroError != nil {
		log.ErrorC(marshalErr, avroError, log.Data{eventKey: e})
		return avroError
	}
	p.Producer.Output() <- bytes
	return nil
}
