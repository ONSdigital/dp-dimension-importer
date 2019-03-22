package main

import (
	"io"
	"io/ioutil"
	"os"

	"context"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/dp-graph/graph"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	datasetHealthCheck "github.com/ONSdigital/go-ns/clients/dataset"
	"github.com/ONSdigital/go-ns/healthcheck"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

type responseBodyReader struct{}

func (r responseBodyReader) Read(reader io.Reader) ([]byte, error) {
	return ioutil.ReadAll(reader)
}

func main() {
	log.Namespace = "dimension-importer"

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := config.Get()
	if err != nil {
		log.ErrorC("config.Load returned an error", err, nil)
		os.Exit(1)
	}

	log.Debug("application configuration", log.Data{"config": cfg})

	// Incoming kafka topic for instances to process
	instanceConsumer := newConsumer(cfg.KafkaAddr, cfg.IncomingInstancesTopic, cfg.IncomingInstancesConsumerGroup)

	// Outgoing topic for instances that have completed processing
	instanceCompleteProducer := newProducer(cfg.KafkaAddr, cfg.OutgoingInstancesTopic)

	// Outgoing topic for any errors while processing an instance
	errorReporterProducer := newProducer(cfg.KafkaAddr, cfg.EventReporterTopic)

	graphDB, err := graph.New(context.Background(), graph.Subsets{Instance: true, Dimension: true})
	if err != nil {
		log.ErrorC("graph.New returned an error", err, nil)
		os.Exit(1)
	}

	// MessageProducer for instanceComplete events.
	instanceCompletedProducer := message.InstanceCompletedProducer{
		Producer:   instanceCompleteProducer,
		Marshaller: schema.InstanceCompletedSchema,
	}

	// ImportAPI HTTP client.
	datasetAPICli := client.DatasetAPI{
		AuthToken:           cfg.ServiceAuthToken,
		DatasetAPIHost:      cfg.DatasetAPIAddr,
		DatasetAPIAuthToken: cfg.DatasetAPIAuthToken,
		HTTPClient:          &http.Client{},
		ResponseBodyReader:  responseBodyReader{},
	}

	// Receiver for NewInstance events.
	instanceEventHandler := &handler.InstanceEventHandler{
		Store:         graphDB,
		DatasetAPICli: datasetAPICli,
		Producer:      instanceCompletedProducer,
	}

	// Errors handler
	errorReporter, err := reporter.NewImportErrorReporter(errorReporterProducer, log.Namespace)
	if err != nil {
		log.ErrorC("reporter.NewImportErrorReporter returned an error", err, nil)
		os.Exit(1)
	}

	healthCheckErrors := make(chan error)

	// HTTP Health check endpoint.
	healthcheckServer := healthcheck.NewServer(
		cfg.BindAddr,
		cfg.HealthCheckInterval,
		healthCheckErrors,
		graphDB,
		datasetHealthCheck.New(cfg.DatasetAPIAddr),
	)

	messageReciever := message.KafkaMessageReciever{
		InstanceHandler: instanceEventHandler,
		ErrorReporter:   errorReporter,
	}

	consumer := message.NewConsumer(instanceConsumer, messageReciever, cfg.GracefulShutdownTimeout)
	consumer.Listen()

	select {
	case err := <-instanceConsumer.Errors():
		log.ErrorC("incoming instance kafka consumer receieved an error, attempting graceful shutdown", err, nil)
	case err := <-instanceCompleteProducer.Errors():
		log.ErrorC("completed instance kafka producer receieved an error, attempting graceful shutdown", err, nil)
	case err := <-errorReporterProducer.Errors():
		log.ErrorC("error reporter kafka producer recieved an error, attempting graceful shutdown", err, nil)
	case err := <-healthCheckErrors:
		log.ErrorC("healthcheck server returned an error, attempting graceful shutdown", err, nil)
	case signal := <-signals:
		log.Info("os signal receieved, attempting graceful shutdown", log.Data{"signal": signal.String()})
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

	instanceConsumer.StopListeningToConsumer(ctx)
	consumer.Close(ctx)
	instanceConsumer.Close(ctx)
	instanceCompleteProducer.Close(ctx)
	graphDB.Close(ctx)
	errorReporterProducer.Close(ctx)
	healthcheckServer.Close(ctx)

	cancel() // stop timer
	log.Info("gracecful shutdown comeplete", nil)
	os.Exit(1)
}

func newConsumer(kafkaAddr []string, topic string, namespace string) *kafka.ConsumerGroup {
	consumer, err := kafka.NewSyncConsumer(kafkaAddr, topic, namespace, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("kafka.NewSyncConsumer returned an error", err, log.Data{
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
		log.ErrorC("kafka.NewProducer returned an error", err, log.Data{"topic": topic})
		os.Exit(1)
	}
	return producer
}
