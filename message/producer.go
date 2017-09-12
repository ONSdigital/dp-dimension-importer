package message

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/go-ns/log"
)

const (
	marshalErr = "unexpected error while attempting to avro marshall event.InstanceCompleted"
)

//go:generate moq -out ./message_test/producer_generated_mocks.go -pkg message_test . Marshaller KafkaProducer

type KafkaProducer interface {
	Output() chan []byte
}

// Marshaller defines a type for marshalling the requested object into the required format.
type Marshaller interface {
	Marshal(s interface{}) ([]byte, error)
}

// InstanceCompletedProducer produces kafka messages for instances which have been successfully processed.
type InstanceCompletedProducer struct {
	Marshaller Marshaller
	Producer   KafkaProducer
}

// Completed produce a kafka message for an instance which has been successfully processed.
func (p InstanceCompletedProducer) Completed(e event.InstanceCompleted) error {
	bytes, avroError := p.Marshaller.Marshal(e)
	if avroError != nil {
		log.ErrorC(marshalErr, avroError, log.Data{eventKey: e})
		return avroError
	}
	p.Producer.Output() <- bytes
	return nil
}
