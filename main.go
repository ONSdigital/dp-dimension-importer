package main

import (
	"os"

	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

var incomingKafka chan kafka.Message

func main() {
	log.Namespace = "dimension-importer"
	cfg, err := config.Load()
	if err != nil {
		os.Exit(1)
	}

	log.Debug("Application configuration", log.Data{
		"": cfg.String(),
	})

	consumer, err := kafka.NewConsumerGroup(cfg.KafkaAddr, cfg.DimensionsExtractedTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("Could not create consumer", err, nil)
		panic("Could not create consumer")
	}

	client.Host = cfg.ImportAddr
	database := client.InitDB(cfg.DatabaseURL, cfg.PoolSize)

	eventHandler := &handler.DimensionsExtractedEventHandler{
		DimensionsStore: database,
		ImportAPI:       client.ImportAPI{},
	}

	err = message.Consume(consumer, eventHandler)
	if err != nil {
		log.ErrorC("consumer", err, nil)
		panic("Consumer returned error")
	}
}
