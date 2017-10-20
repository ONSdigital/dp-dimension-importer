package message

import (
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
)

var loggerP = logging.Logger{Prefix: "message.InstanceCompletedProducer"}

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
		return errors.Wrap(avroError, fmt.Sprintf("Marshaller.Marshal returned an error: event=%v", e))
	}
	p.Producer.Output() <- bytes
	loggerP.Info("completed successfully", log.Data{"event": e})
	return nil
}
