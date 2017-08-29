package message

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

const (
	marshalErr = "Unexpected error while attempting to avro marshall event.DimensionsInsertedEvent"
)

//go:generate moq -out ./message_test/producer_generated_mocks.go -pkg message_test . Marshaller

// Marshaller defines a type for marshalling the requested object into the required format.
type Marshaller interface {
	Marshal(s interface{}) ([]byte, error)
}

// DimensionInsertedProducer type for producing DimensionsInsertedEvents to a kafka topic.
type DimensionInsertedProducer struct {
	Marshaller Marshaller
	Producer   kafka.MessageProducer
}

// DimensionInserted kafka message to complete dimension inserted event
func (p DimensionInsertedProducer) DimensionInserted(e event.DimensionsInsertedEvent) error {
	bytes, avroError := p.Marshaller.Marshal(e)
	if avroError != nil {
		log.ErrorC(marshalErr, avroError, log.Data{eventKey: e})
		return avroError
	}
	p.Producer.Output() <- bytes
	return nil
}
