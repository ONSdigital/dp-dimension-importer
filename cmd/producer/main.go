package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/v2/log"
)

var instanceID = flag.String("instance", "5156253b-e21e-4a73-a783-fb53fabc1211", "")
var file = flag.String("file", "s3://dp-frontend-florence-file-uploads/159-coicopcomb-inc-geo_cutcsv", "")

var topic = flag.String("topic", "dimensions-extracted", "")
var kafkaHost = flag.String("kafka", "localhost:9092", "")
var maxBytes = int(2000000)

func main() {
	flag.Parse()
	ctx := context.Background()

	var brokers []string
	brokers = append(brokers, *kafkaHost)

	pConfig := &kafka.ProducerConfig{
		MaxMessageBytes: &maxBytes,
	}

	producer, err := kafka.NewProducer(ctx, brokers, *topic, kafka.CreateProducerChannels(), pConfig)
	if err != nil {
		log.Fatal(ctx, "error creating producer", err)
		os.Exit(1)
	}

	dimensionsInsertedEvent := event.NewInstance{
		InstanceID: *instanceID,
		FileURL:    *file,
	}

	bytes, err := schema.NewInstanceSchema.Marshal(dimensionsInsertedEvent)
	if err != nil {
		log.Fatal(ctx, "error marshalling dimensions inserted event", err)
		os.Exit(1)
	}
	producer.Channels().Output <- bytes

	// give Kafka time to produce the message before closing the producer
	time.Sleep(time.Second)

	if err := producer.Close(ctx); err != nil {
		log.Fatal(ctx, "error closing producer", err)
		os.Exit(1)
	}
}
