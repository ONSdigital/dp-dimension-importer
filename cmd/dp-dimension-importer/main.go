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
	"context"
)

const (
	consumerErrMsg           = "kafka Consumer Error recieved"
	createConsumerErr        = "error while attempting to create kafka consumer"
	producerErrMsg           = "completed instance producer error recieved"
	eventReporterErrMsg      = "event reporter producer error recieved"
	createProducerErr        = "error while attempting to create kafka producer"
	createConnPoolErr        = "unexpected error while to create database connection pool"
	gracefulShutdownMsg      = "commencing graceful shutdown..."
	gracefulShutdownComplete = "graceful shutdown completed successfully, exiting application"
	loadConfigErr            = "error while loading application config."
)

type responseBodyReader struct{}

func (r responseBodyReader) Read(reader io.Reader) ([]byte, error) {
	return ioutil.ReadAll(reader)
}

func main() {
	log.Namespace = "dimension-importer"

	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := config.Load()
	if err != nil {
		log.ErrorC(loadConfigErr, err, nil)
		os.Exit(1)
	}

	log.Debug("application configuration", log.Data{"config": cfg})

	// Incoming kafka topic for instances to process
	instanceConsumer := newConsumer(cfg.KafkaAddr, cfg.IncomingInstancesTopic, log.Namespace)

	// Outgoing topic for instances that have completed processing
	instanceCompleteProducer := newProducer(cfg.KafkaAddr, cfg.OutgoingInstancesTopic)

	// Outgoing topic for any errors while processing an instance
	errorEventProducer := newProducer(cfg.KafkaAddr, cfg.EventReporterTopic)

	neo4jClient := createNeo4jClient(cfg)

	newDimensionInserterFunc := func() handler.DimensionRepository {
		return repository.NewDimensionRepository(neo4jClient, map[string]string{})
	}

	// ImportAPI HTTP client.
	datasetAPICli := client.DatasetAPI{
		DatasetAPIHost:      cfg.DatasetAPIAddr,
		DatasetAPIAuthToken: cfg.DatasetAPIAuthToken,
		HTTPClient:          &http.Client{},
		ResponseBodyReader:  responseBodyReader{},
	}

	// Handler for dimensionsExtracted events.
	instanceEventHandler := &handler.InstanceEventHandler{
		NewDimensionInserter: newDimensionInserterFunc,
		DatasetAPICli:        datasetAPICli,
		InstanceRepository:   &repository.InstanceRepository{Neo4j: neo4jClient},
	}

	// MessageProducer for dimensionsInsertedEvents
	instanceCompletedProducer := message.InstanceCompletedProducer{
		Producer:   instanceCompleteProducer,
		Marshaller: schema.InstanceCompletedSchema,
	}

	// Errors handler
	errorEventHandler := &handler.ErrorHandler{
		Producer:   errorEventProducer,
		Marshaller: schema.ErrorEventSchema,
	}

	healthCheckErrors := make(chan error)

	// HTTP Health check endpoint.
	healthcheck.NewHandler(cfg.BindAddr, healthCheckErrors)

	ctx, cancelConsumerLoop := context.WithCancel(context.Background())

	// Gracefully shutdown the application closing any open resources.
	gracefulShutdown := func() {
		log.Info(gracefulShutdownMsg, nil)
		shutdownCTX, _ := context.WithTimeout(ctx, cfg.ShutdownTimeout)

		cancelConsumerLoop()
		instanceConsumer.Close(shutdownCTX)
		instanceCompleteProducer.Close(shutdownCTX)
		errorEventProducer.Close(shutdownCTX)
		healthcheck.Close(shutdownCTX)

		log.Info(gracefulShutdownComplete, nil)
		os.Exit(1)
	}

	// run the consumer
	message.Consume(ctx, instanceConsumer, instanceCompletedProducer, instanceEventHandler, errorEventHandler)

	for {
		select {
		case err := <-instanceConsumer.Errors():
			log.ErrorC(consumerErrMsg, err, log.Data{logKeys.ErrorDetails: err})
			gracefulShutdown()
		case err := <-instanceCompleteProducer.Errors():
			log.ErrorC(producerErrMsg, err, nil)
			gracefulShutdown()
		case err := <-errorEventProducer.Errors():
			log.ErrorC(eventReporterErrMsg, err, nil)
			gracefulShutdown()
		case err := <-healthCheckErrors:
			log.ErrorC("receieved error healthcheck server", err, nil)
			gracefulShutdown()
		case <-signals:
			log.Info("Signal intercepted", nil)
			gracefulShutdown()
		}
	}
}

func newConsumer(kafkaAddr []string, topic string, namespace string) *kafka.ConsumerGroup {
	consumer, err := kafka.NewConsumerGroup(kafkaAddr, topic, namespace, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC(createConsumerErr, err, nil)
		os.Exit(1)
	}
	return consumer
}

func newProducer(kafkaAddr []string, topic string) kafka.Producer {
	producer, err := kafka.NewProducer(kafkaAddr, topic, 0)
	if err != nil {
		log.ErrorC(createProducerErr, err, log.Data{logKeys.KafkaTopic: topic})
		os.Exit(1)
	}
	return producer
}

func createNeo4jClient(cfg *config.Config) *client.Neo4j {
	var neo4jCli *client.Neo4j
	var err error

	if neo4jCli, err = client.NewNeo4j(cfg.DatabaseURL, cfg.PoolSize); err != nil {
		log.ErrorC(createConnPoolErr, err, log.Data{
			logKeys.URL:      cfg.DatabaseURL,
			logKeys.PoolSize: cfg.PoolSize,
		})
		os.Exit(1)
	}
	return neo4jCli
}
