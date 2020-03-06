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
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/server"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

type responseBodyReader struct{}

var (
	// BuildTime represents the time in which the service was built
	BuildTime string
	// GitCommit represents the commit (SHA-1) hash of the service that is running
	GitCommit string
	// Version represents the version of the service that is running
	Version string
)

func (r responseBodyReader) Read(reader io.Reader) ([]byte, error) {
	return ioutil.ReadAll(reader)
}

func main() {
	log.Namespace = "dimension-importer"
	ctx := context.Background()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := config.Get()
	if err != nil {
		log.Event(ctx, "config.Load returned an error", log.FATAL, log.Error(err))
		os.Exit(1)
	}

	log.Event(ctx, "application configuration", log.INFO, log.Data{"config": cfg})

	// Incoming kafka topic for instances to process
	instanceConsumer := newConsumer(cfg.KafkaAddr, cfg.IncomingInstancesTopic, cfg.IncomingInstancesConsumerGroup)

	// Outgoing topic for instances that have completed processing
	instanceCompleteProducer := newProducer(cfg.KafkaAddr, cfg.OutgoingInstancesTopic)

	// Outgoing topic for any errors while processing an instance
	errorReporterProducer := newProducer(cfg.KafkaAddr, cfg.EventReporterTopic)

	graphDB, err := graph.New(ctx, graph.Subsets{Instance: true, Dimension: true})
	if err != nil {
		log.Event(ctx, "graph.New returned an error", log.FATAL, log.Error(err))
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
		log.Event(ctx, "reporter.NewImportErrorReporter returned an error", log.FATAL, log.Error(err))
		os.Exit(1)
	}

	// Create healthcheck object with versionInfo
	versionInfo, err := healthcheck.NewVersionInfo(BuildTime, GitCommit, Version)
	if err != nil {
		log.Event(ctx, "Failed to create versionInfo for healthcheck", log.FATAL, log.Error(err))
		os.Exit(1)
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckRecoveryInterval, cfg.HealthCheckInterval)
	if err := registerCheckers(&hc); err != nil {
		os.Exit(1)
	}

	chHttpServerDone := make(chan error)
	httpServer := startHealthCheck(ctx, &hc, cfg.BindAddr, chHttpServerDone)

	// TODO use new healthcheck

	// // HTTP Health check endpoint.
	// healthcheckServer := healthcheck.NewServer(
	// 	cfg.BindAddr,
	// 	cfg.HealthCheckInterval,
	// 	healthCheckErrors,
	// 	graphDB,
	// 	datasetHealthCheck.New(cfg.DatasetAPIAddr),
	// )

	messageReciever := message.KafkaMessageReciever{
		InstanceHandler: instanceEventHandler,
		ErrorReporter:   errorReporter,
	}

	consumer := message.NewConsumer(instanceConsumer, messageReciever, cfg.GracefulShutdownTimeout)
	consumer.Listen()

	// TODO non-fatal errors should do logging instead of triggering shutdown
	select {
	case err := <-instanceConsumer.Errors():
		log.Event(ctx, "incoming instance kafka consumer receieved an error, attempting graceful shutdown", log.ERROR, log.Error(err))
	case err := <-instanceCompleteProducer.Errors():
		log.Event(ctx, "completed instance kafka producer receieved an error, attempting graceful shutdown", log.ERROR, log.Error(err))
	case err := <-errorReporterProducer.Errors():
		log.Event(ctx, "error reporter kafka producer recieved an error, attempting graceful shutdown", log.ERROR, log.Error(err))
	case signal := <-signals:
		log.Event(ctx, "os signal receieved, attempting graceful shutdown", log.INFO, log.Data{"signal": signal.String()})
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, cfg.GracefulShutdownTimeout)

	hc.Stop()

	StopHealthCheck(shutdownCtx, &hc, httpServer)

	instanceConsumer.StopListeningToConsumer(shutdownCtx)
	consumer.Close(shutdownCtx)
	instanceConsumer.Close(shutdownCtx)
	instanceCompleteProducer.Close(shutdownCtx)
	graphDB.Close(shutdownCtx)
	errorReporterProducer.Close(shutdownCtx)
	// healthcheckServer.Close(shutdownCtx)

	cancel() // stop timer
	log.Event(ctx, "graceful shutdown complete", log.INFO)
	os.Exit(1)
}

func newConsumer(kafkaAddr []string, topic string, namespace string) *kafka.ConsumerGroup {
	consumer, err := kafka.NewSyncConsumer(kafkaAddr, topic, namespace, kafka.OffsetNewest)
	if err != nil {
		log.Event(context.Background(), "kafka.NewSyncConsumer returned an error", log.FATAL, log.Error(err), log.Data{
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
		log.Event(context.Background(), "kafka.NewProducer returned an error", log.FATAL, log.Error(err), log.Data{"topic": topic})
		os.Exit(1)
	}
	return producer
}

// StartHealthCheck sets up the Handler, starts the healthcheck and the http server that serves health endpoint
func startHealthCheck(ctx context.Context, hc *healthcheck.HealthCheck, bindAddr string, serverDone chan error) *server.Server {
	router := mux.NewRouter()
	router.Path("/health").HandlerFunc(hc.Handler)
	hc.Start(ctx)

	httpServer := server.New(bindAddr, router)
	httpServer.HandleOSSignals = false

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Event(ctx, "", log.ERROR, log.Error(err))
			hc.Stop()
			serverDone <- err
		}
		close(serverDone)
	}()
	return httpServer
}

// RegisterCheckers adds the checkers for the provided clients to the healthcheck object.
func registerCheckers(hc *healthcheck.HealthCheck) (err error) {
	return nil
}

// StopHealthCheck shuts down the http listener
func StopHealthCheck(ctx context.Context, hc *healthcheck.HealthCheck, httpServer *server.Server) (err error) {
	err = httpServer.Shutdown(ctx)
	hc.Stop()
	return
}
