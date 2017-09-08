package main

import (
	"os"

	"github.com/ONSdigital/dp-dimension-importer/client"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/repository"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"io"
	"io/ioutil"
	"net/http"
	"github.com/ONSdigital/dp-dimension-importer/healthcheck"
	"os/signal"
	"syscall"
	"time"
	"context"
)

const (
	conumserErrMsg           = "Kafka Consumer Error recieved"
	createConsumerErr        = "Error while attempting to create kafka consumer"
	producerErrMsg           = "Completed Error recieved"
	createProducerErr        = "Error while attempting to create kafka producer"
	createConnPoolErr        = "unexpected error while to create database connection pool"
	errorEventsProducerErr   = "Error while attempting to create kafka producer"
	gracefulShutdownMsg      = "Commencing graceful shutdown..."
	gracefulShutdownComplete = "Graceful shutdown completed successfully, exiting application"
)

type responseBodyReader struct{}

func (r responseBodyReader) Read(reader io.Reader) ([]byte, error) {
	return ioutil.ReadAll(reader)
}

func main() {
	log.Namespace = "dimension-importer"

	errorsChan := make(chan error)
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := config.Load()

	log.Debug("application configuration", log.Data{"config": cfg})

	incomingInstances, err := kafka.NewConsumerGroup(cfg.KafkaAddr, cfg.DimensionsExtractedTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC(createConsumerErr, err, nil)
		os.Exit(1)
	}

	outgoingInstances, err := kafka.NewProducer(cfg.KafkaAddr, cfg.DimensionsInsertedTopic, 0)
	if err != nil {
		log.ErrorC(createProducerErr, err, log.Data{logKeys.KafkaTopic: cfg.DimensionsInsertedTopic})
		os.Exit(1)
	}

	errorEventProducer, err := kafka.NewProducer(cfg.KafkaAddr, "event-reporter", 0)
	if err != nil {
		log.ErrorC(errorEventsProducerErr, err, log.Data{logKeys.KafkaTopic: cfg.DimensionsInsertedTopic})
		os.Exit(1)
	}

	var neo4jClient *client.Neo4j
	if neo4jClient, err = client.NewNeo4j(cfg.DatabaseURL, cfg.PoolSize); err != nil {
		log.ErrorC(createConnPoolErr, err, log.Data{
			logKeys.URL:      cfg.DatabaseURL,
			logKeys.PoolSize: cfg.PoolSize,
		})
		os.Exit(1)
	}

	newDimensionInserterFunc := func() handler.DimensionRepository {
		return repository.NewDimensionRepository(neo4jClient, map[string]string{})
	}

	// ImportAPI HTTP client.
	importAPI := client.NewImportAPI(cfg.ImportAddr, cfg.ImportAuthToken, responseBodyReader{}, &http.Client{})

	// Handler for dimensionsExtracted events.
	eventHandler := handler.NewDimensionExtractedEventHandler(
		newDimensionInserterFunc,
		&repository.InstanceRepository{Neo4j: neo4jClient},
		importAPI)

	// MessageProducer for dimensionsInsertedEvents
	instanceCompletedProducer := message.NewInstanceCompletedProducer(
		outgoingInstances,
		schema.InstanceCompletedSchema,
	)

	// Errors handler
	errorEventHandler := &handler.ErrorHandler{
		Producer:   errorEventProducer,
		Marshaller: schema.ErrorEventSchema,
	}

	// HTTP Health check endpoint.
	healthcheck.NewHandler()

	gracefulShutdown := func() {
		log.Info(gracefulShutdownMsg, nil)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		message.CloseConsumer()
		incomingInstances.Close(ctx)
		outgoingInstances.Close(ctx)
		errorEventProducer.Close(ctx)
		healthcheck.Close(ctx)

		log.Info(gracefulShutdownComplete, nil)
		os.Exit(0)
	}

	// run the consumer
	message.Consume(incomingInstances, instanceCompletedProducer, eventHandler, errorEventHandler)

	for {
		select {
		case err := <-incomingInstances.Errors():
			log.ErrorC(conumserErrMsg, err, log.Data{logKeys.ErrorDetails: err})
			gracefulShutdown()
		case err := <-outgoingInstances.Errors():
			log.ErrorC(producerErrMsg, err, nil)
			gracefulShutdown()
		case err := <-errorsChan:
			log.ErrorC("errorChan receieved an error", err, nil)
			gracefulShutdown()
		case <-signals:
			log.Info("Signal intercepted", nil)
			gracefulShutdown()
		}
	}
}
