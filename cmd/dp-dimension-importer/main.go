package main

import (
	"os"

	"github.com/ONSdigital/dp-dimension-importer/client"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/repository"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"io"
	"io/ioutil"
	"net/http"
)

type responseBodyReader struct{}

func (r responseBodyReader) Read(reader io.Reader) ([]byte, error) {
	return ioutil.ReadAll(reader)
}

func main() {
	log.Namespace = "dimension-importer"
	cfg, err := config.Load()
	if err != nil {
		os.Exit(1)
	}

	log.Debug("Application configuration", log.Data{"config": cfg})

	consumer, err := kafka.NewConsumerGroup(cfg.KafkaAddr, cfg.DimensionsExtractedTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("Could not create consumer", err, nil)
		os.Exit(1)
	}

	insertedEventProducer, err := kafka.NewProducer(cfg.KafkaAddr, cfg.DimensionsInsertedTopic, 0)
	if err != nil {
		log.ErrorC("kafka producer error", err, log.Data{"topic": cfg.DimensionsInsertedTopic})
		os.Exit(1)
	}

	var neo4jClient *client.Neo4j
	if neo4jClient, err = client.NewNeo4j(cfg.DatabaseURL, cfg.PoolSize); err != nil {
		log.ErrorC("Unexpected error while to create database connection pool", err, log.Data{
			logKeys.URL:      cfg.DatabaseURL,
			logKeys.PoolSize: cfg.PoolSize,
		})
		os.Exit(1)
	}

	newDimensionInserterFunc := func() handler.DimensionRepository {
		return repository.DimensionRepository{
			Neo4jCli:         *neo4jClient,
			ConstraintsCache: map[string]string{},
		}
	}

	importAPI := client.ImportAPI{
		ResponseBodyReader: responseBodyReader{},
		HTTPClient:         &http.Client{},
		AuthToken:          cfg.ImportAuthToken,
		ImportHost:         cfg.ImportAddr,
	}

	eventHandler := &handler.DimensionsExtractedEventHandler{
		NewDimensionInserter: newDimensionInserterFunc,
		InstanceRepository:   &repository.InstanceRepository{Neo4j: neo4jClient},
		ImportAPI:            importAPI,
	}

	err = message.Consume(consumer, insertedEventProducer, eventHandler)
	if err != nil {
		log.ErrorC("consumer", err, nil)
		os.Exit(1)
	}
}
