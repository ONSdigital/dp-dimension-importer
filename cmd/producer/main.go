package main

import (
	"context"
	"flag"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	kafka "github.com/ONSdigital/dp-kafka"
)

var instanceID = flag.String("instance", "5156253b-e21e-4a73-a783-fb53fabc1211", "")
var file = flag.String("file", "s3://dp-frontend-florence-file-uploads/159-coicopcomb-inc-geo_cutcsv", "")

var topic = flag.String("topic", "dimensions-extracted", "")
var kafkaHost = flag.String("kafka", "localhost:9092", "")

func main() {

	flag.Parse()
	ctx := context.Background()

	var brokers []string
	brokers = append(brokers, *kafkaHost)

	producer, _ := kafka.NewProducer(ctx, brokers, *topic, int(2000000), kafka.CreateProducerChannels())
	dimensionsInsertedEvent := event.NewInstance{
		InstanceID: *instanceID,
		FileURL:    *file,
	}

	bytes, error := schema.NewInstanceSchema.Marshal(dimensionsInsertedEvent)
	if error != nil {
		panic(error)
	}
	producer.Channels().Output <- bytes

	// give Kafka time to produce the message before closing the producer
	time.Sleep(time.Second)

	producer.Close(nil)
}
