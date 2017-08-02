package main

import (
	"os"

	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/dp-dimension-importer/logging"
)

var incomingKafka chan kafka.Message

func main() {
	log.Namespace = "dimension-importer"
	cfg, err := config.Load()
	if err != nil {
		os.Exit(1)
	}

	logging.Init(cfg.LogLevel)
	logging.Debug.Printf("Application configuration:, %v", cfg)

	consumer, err := kafka.NewConsumerGroup(cfg.KafkaAddr, cfg.DimensionsExtractedTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("Could not create consumer", err, nil)
		panic("Could not create consumer")
	}

	client.Host = cfg.ImportAddr
	database := client.InitialiseDatabaseClient(cfg.DatabaseURL, cfg.PoolSize)

	eventHandler := &handler.DimensionsExtractedEventHandler{
		DimensionsStore: database,
		ImportAPI:       client.ImportAPI{},
		BatchSize:       100, // TODO Move to config
	}

	err = message.Consume(consumer, eventHandler)
	if err != nil {
		log.ErrorC("consumer", err, nil)
		panic("Consumer returned error")
	}
}
