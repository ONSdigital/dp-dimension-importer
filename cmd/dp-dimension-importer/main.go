package main

import (
	"errors"
	"os"

	"context"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/initialise"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/dp-dimension-importer/store"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/server"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

var (
	// BuildTime represents the time in which the service was built
	BuildTime string
	// GitCommit represents the commit (SHA-1) hash of the service that is running
	GitCommit string
	// Version represents the version of the service that is running
	Version string
)

func main() {
	log.Namespace = "dimension-importer"
	ctx := context.Background()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := config.Get(ctx)
	if err != nil {
		log.Event(ctx, "config.Load returned an error", log.FATAL, log.Error(err))
		os.Exit(1)
	}

	log.Event(ctx, "application configuration", log.INFO, log.Data{"config": cfg})

	// serviceList keeps track of what dependency services have been initialised
	serviceList := initialise.ExternalServiceList{}

	// Incoming kafka topic for instances to process
	instanceConsumer, err := serviceList.GetConsumer(ctx, cfg)
	if err != nil {
		os.Exit(1)
	}

	// Outgoing topic for instances that have completed processing
	instanceCompleteProducer, err := serviceList.GetProducer(ctx, cfg, initialise.InstanceComplete)
	if err != nil {
		os.Exit(1)
	}

	// Outgoing topic for any errors while processing an instance
	errorReporterProducer, err := serviceList.GetProducer(ctx, cfg, initialise.ErrorReporter)
	if err != nil {
		os.Exit(1)
	}

	// Connection to graph DB
	graphDB, err := serviceList.GetGraphDB(ctx)
	if err != nil {
		os.Exit(1)
	}

	// MessageProducer for instanceComplete events.
	instanceCompletedProducer := message.InstanceCompletedProducer{
		Producer:   instanceCompleteProducer,
		Marshaller: schema.InstanceCompletedSchema,
	}

	// Dataset Client wrapper.
	datasetAPICli, err := client.NewDatasetAPIClient(cfg.ServiceAuthToken, cfg.DatasetAPIAddr)

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
	hc, err := serviceList.GetHealthChecker(ctx, BuildTime, GitCommit, Version, cfg)
	if err != nil {
		os.Exit(1)
	}

	if err := registerCheckers(hc, instanceConsumer, instanceCompleteProducer, errorReporterProducer, datasetAPICli.Client, graphDB); err != nil {
		os.Exit(1)
	}

	httpServer := startHealthCheck(ctx, hc, cfg.BindAddr)

	messageReceiver := message.KafkaMessageReceiver{
		InstanceHandler: instanceEventHandler,
		ErrorReporter:   errorReporter,
	}

	// Create Consumer with kafkaConsmer
	consumer := serviceList.NewConsumer(ctx, instanceConsumer, messageReceiver, cfg.GracefulShutdownTimeout)
	consumer.Listen()

	instanceConsumer.Channels().LogErrors(ctx, "incoming instance kafka consumer received an error")
	instanceCompleteProducer.Channels().LogErrors(ctx, "completed instance kafka producer received an error")
	errorReporterProducer.Channels().LogErrors(ctx, "error reporter kafka producer received an error")

	// If we receive a signal (SIGINT or SIGTERM), start graceful shutdown
	signal := <-signals
	log.Event(ctx, "os signal received, attempting graceful shutdown", log.INFO, log.Data{"signal": signal.String()})

	shutdownCtx, cancel := context.WithTimeout(ctx, cfg.GracefulShutdownTimeout)

	if serviceList.HealthCheck {
		log.Event(shutdownCtx, "stopping healthcheck", log.INFO)
		hc.Stop()
	}
	logIfError(shutdownCtx, httpServer.Shutdown(shutdownCtx))

	if serviceList.InstanceConsumer {
		log.Event(shutdownCtx, "stop listening to instance kafka consumer", log.INFO)
		logIfError(shutdownCtx, instanceConsumer.StopListeningToConsumer(shutdownCtx))
	}

	if serviceList.Consumer {
		log.Event(shutdownCtx, "closing event consumer", log.INFO)
		consumer.Close(shutdownCtx)
	}

	if serviceList.InstanceConsumer {
		log.Event(shutdownCtx, "closing instance kafka consumer", log.INFO)
		logIfError(shutdownCtx, instanceConsumer.Close(shutdownCtx))
	}

	if serviceList.InstanceCompleteProducer {
		log.Event(shutdownCtx, "closing instance complete kafka producer")
		logIfError(shutdownCtx, instanceCompleteProducer.Close(shutdownCtx))
	}

	if serviceList.GraphDB {
		log.Event(shutdownCtx, "closing graphDB")
		logIfError(shutdownCtx, graphDB.Close(shutdownCtx))
	}

	if serviceList.ErrorReporterProducer {
		log.Event(shutdownCtx, "closing error reporter kafka producer")
		logIfError(shutdownCtx, errorReporterProducer.Close(shutdownCtx))
	}

	cancel() // stop timer
	log.Event(ctx, "graceful shutdown complete", log.INFO)
	os.Exit(0)
}

// StartHealthCheck sets up the Handler, starts the healthcheck and the http server that serves health endpoint
func startHealthCheck(ctx context.Context, hc *healthcheck.HealthCheck, bindAddr string) *server.Server {
	router := mux.NewRouter()
	router.Path("/health").HandlerFunc(hc.Handler)
	hc.Start(ctx)

	httpServer := server.New(bindAddr, router)
	httpServer.HandleOSSignals = false

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Event(ctx, "", log.ERROR, log.Error(err))
			hc.Stop()
		}
	}()
	return httpServer
}

// RegisterCheckers adds the checkers for the provided clients to the healthcheck object.
func registerCheckers(hc *healthcheck.HealthCheck,
	instanceConsumer *kafka.ConsumerGroup,
	instanceCompleteProducer *kafka.Producer,
	errorReporterProducer *kafka.Producer,
	datasetClient client.IClient,
	db store.Storer) (err error) {

	hasErrors := false

	if err = hc.AddCheck("Kafka Instance Consumer", instanceConsumer.Checker); err != nil {
		hasErrors = true
		log.Event(nil, "Error Adding Check for Kafka Instance Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka InstanceComplete Producer", instanceCompleteProducer.Checker); err != nil {
		hasErrors = true
		log.Event(nil, "Error Adding Check for Kafka Instance Complete Producer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka ErrorReporter Producer", errorReporterProducer.Checker); err != nil {
		hasErrors = true
		log.Event(nil, "Error Adding Check for Kafka Error Reporter Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Dataset", datasetClient.Checker); err != nil {
		hasErrors = true
		log.Event(nil, "Error Adding Check for Dataset Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Neo4J", db.Checker); err != nil {
		hasErrors = true
		log.Event(nil, "Error Adding Check for GraphDB", log.ERROR, log.Error(err))
	}

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}
	return nil
}

func logIfError(ctx context.Context, err error) {
	if err != nil {
		log.Event(ctx, "error", log.ERROR, log.Error(err))
		return
	}
}
