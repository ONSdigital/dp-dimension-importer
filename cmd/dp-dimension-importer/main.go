package main

import (
	"os"

	"context"
	"io"
	"io/ioutil"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dimension-importer/client"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/healthcheck"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/repository"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
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
		log.ErrorC("config.Load returned an error", err, nil)
		os.Exit(1)
	}

	log.Debug("application configuration", log.Data{"config": cfg})

	// Incoming kafka topic for instances to process
	instanceConsumer := newConsumer(cfg.KafkaAddr, cfg.IncomingInstancesTopic, log.Namespace)

	// Outgoing topic for instances that have completed processing
	instanceCompleteProducer := newProducer(cfg.KafkaAddr, cfg.OutgoingInstancesTopic)

	// Outgoing topic for any errors while processing an instance
	errorReporterProducer := newProducer(cfg.KafkaAddr, cfg.EventReporterTopic)

	neo4jCli := client.Neo4j{}

	connectionPool, err := repository.NewConnectionPool(cfg.DatabaseURL, cfg.PoolSize)
	if err != nil {
		log.ErrorC("repository.NewConnectionPool returned an error", err, log.Data{
			logKeys.URL:      cfg.DatabaseURL,
			logKeys.PoolSize: cfg.PoolSize,
		})
		os.Exit(1)
	}

	newDimensionInserterFunc := func() (handler.DimensionRepository, error) {
		return repository.NewDimensionRepository(connectionPool, neo4jCli)
	}

	newInstanceRepoFunc := func() (handler.InstanceRepository, error) {
		return repository.NewInstanceRepository(connectionPool, neo4jCli)
	}

	// MessageProducer for instanceComplete events.
	instanceCompletedProducer := message.InstanceCompletedProducer{
		Producer:   instanceCompleteProducer,
		Marshaller: schema.InstanceCompletedSchema,
	}

	// ImportAPI HTTP client.
	datasetAPICli := client.DatasetAPI{
		DatasetAPIHost:      cfg.DatasetAPIAddr,
		DatasetAPIAuthToken: cfg.DatasetAPIAuthToken,
		HTTPClient:          &http.Client{},
		ResponseBodyReader:  responseBodyReader{},
	}

	// Reciever for NewInstance events.
	instanceEventHandler := &handler.InstanceEventHandler{
		NewDimensionInserter:  newDimensionInserterFunc,
		NewInstanceRepository: newInstanceRepoFunc,
		DatasetAPICli:         datasetAPICli,
		Producer:              instanceCompletedProducer,
	}

	// Errors handler
	errorReporter, err := reporter.NewImportErrorReporter(errorReporterProducer, log.Namespace)
	if err != nil {
		log.ErrorC("reporter.NewImportErrorReporter returned an error", err, nil)
		os.Exit(1)
	}

	healthCheckErrors := make(chan error)

	// HTTP Health check endpoint.
	healthcheck.NewHandler(cfg.BindAddr, healthCheckErrors)

	messageReciever := message.KafkaMessageReciever{
		InstanceHandler: instanceEventHandler,
		ErrorReporter:   errorReporter,
	}

	consumer := message.NewConsumer(instanceConsumer, messageReciever, cfg.GracefulShutdownTimeout)
	consumer.Listen()

	for {
		select {
		case err := <-instanceConsumer.Errors():
			log.ErrorC("incoming instance kafka consumer receieved an error, attempting graceful shutdown", err, log.Data{logKeys.ErrorDetails: err})
		case err := <-instanceCompleteProducer.Errors():
			log.ErrorC("completed instance kafka producer receieved an error, attempting graceful shutdown", err, nil)
		case err := <-errorReporterProducer.Errors():
			log.ErrorC("error reporter kafka producer recieved an error, attempting graceful shutdown", err, nil)
		case err := <-healthCheckErrors:
			log.ErrorC("healthcheck server returned an error, attempting graceful shutdown", err, nil)
		case signal := <-signals:
			log.Info("os signal receieved, attempting graceful shutdown", log.Data{"signal": signal.String()})
		}

		ctx, _ := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

		consumer.Close(ctx)
		instanceConsumer.Close(ctx)
		instanceCompleteProducer.Close(ctx)
		errorReporterProducer.Close(ctx)
		healthcheck.Close(ctx)

		log.Info("gracecful shutdown comeplete", nil)
		os.Exit(1)
	}
}

func newConsumer(kafkaAddr []string, topic string, namespace string) *kafka.ConsumerGroup {
	consumer, err := kafka.NewConsumerGroup(kafkaAddr, topic, namespace, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("kafka.NewConsumerGroup returned an error", err, log.Data{
			"brokers":        kafkaAddr,
			"topic":          topic,
			"consumer_group": namespace,
		})
		os.Exit(1)
	}
	return consumer
}

func newProducer(kafkaAddr []string, topic string) kafka.Producer {
	producer, err := kafka.NewProducer(kafkaAddr, topic, 0)
	if err != nil {
		log.ErrorC("kafka.NewProducer returned an error", err, log.Data{logKeys.KafkaTopic: topic})
		os.Exit(1)
	}
	return producer
}
