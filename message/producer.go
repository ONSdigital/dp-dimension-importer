package message

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/event"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/log"
	"github.com/pkg/errors"
)

//go:generate moq -out ./mock/producer.go -pkg mock . Marshaller

// Marshaller defines a type for marshalling the requested object into the required format.
type Marshaller interface {
	Marshal(s interface{}) ([]byte, error)
}

// InstanceCompletedProducer produces kafka messages for instances which have been successfully processed.
type InstanceCompletedProducer struct {
	Marshaller Marshaller
	Producer   kafka.IProducer
}

// Completed produce a kafka message for an instance which has been successfully processed.
func (p InstanceCompletedProducer) Completed(ctx context.Context, e event.InstanceCompleted) error {
	bytes, avroError := p.Marshaller.Marshal(e)
	if avroError != nil {
		return errors.Wrap(avroError, fmt.Sprintf("Marshaller.Marshal returned an error: event=%v", e))
	}
	p.Producer.Channels().Output <- bytes
	log.Event(ctx, "completed successfully", log.INFO, log.Data{"event": e, "package": "message.InstanceCompletedProducer"})
	return nil
}
