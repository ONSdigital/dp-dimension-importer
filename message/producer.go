package message

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
)

// Produce kafka message to complete dimension inserted event
func Produce(producer kafka.MessageProducer, instanceID, fileURL string) error {
	message := model.DimensionsInsertedEvent{InstanceID: instanceID, FileURL: fileURL}
	bytes, avroError := schema.DimensionsInsertedSchema.Marshal(message)
	if avroError != nil {
		return avroError
	}
	producer.Output() <- bytes
	return nil
}
