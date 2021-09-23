package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/initialise"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/dp-dimension-importer/store"
	"github.com/ONSdigital/dp-graph/v2/graph"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/log.go/v2/log"
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
		log.Fatal(ctx, "config load returned an error", err)
		os.Exit(1)
	}

	log.Info(ctx, "application configuration", log.Data{"config": cfg})

	// serviceList keeps track of what dependency services have been initialised
	serviceList := initialise.ExternalServiceList{}

	// Incoming kafka topic for instances to process
	instanceConsumer, err := serviceList.GetConsumer(ctx, cfg)
	if err != nil {
		os.Exit(1)
	}

	// Outgoing topic for instances that have completed processing
	instanceCompleteProducer, err := serviceList.GetProducer(ctx, cfg.OutgoingInstancesTopic, initialise.InstanceComplete, cfg)
	if err != nil {
		os.Exit(1)
	}

	// Outgoing topic for any errors while processing an instance
	errorReporterProducer, err := serviceList.GetProducer(ctx, cfg.EventReporterTopic, initialise.ErrorReporter, cfg)
	if err != nil {
		os.Exit(1)
	}

	// Connection to graph DB
	graphDB, err := serviceList.GetGraphDB(ctx)
	if err != nil {
		os.Exit(1)
	}

	var graphErrorConsumer *graph.ErrorConsumer
	if serviceList.GraphDB {
		graphErrorConsumer = graph.NewLoggingErrorConsumer(ctx, graphDB.ErrorChan())
	}

	// MessageProducer for instanceComplete events.
	instanceCompletedProducer := message.InstanceCompletedProducer{
		Producer:   instanceCompleteProducer,
		Marshaller: schema.InstanceCompletedSchema,
	}

	// Dataset Client wrapper.
	datasetAPICli, err := client.NewDatasetAPIClient(cfg)

	// Receiver for NewInstance events.
	instanceEventHandler := &handler.InstanceEventHandler{
		Store:             graphDB,
		DatasetAPICli:     datasetAPICli,
		Producer:          instanceCompletedProducer,
		BatchSize:         cfg.BatchSize,
		EnablePatchNodeID: cfg.EnablePatchNodeID,
	}

	// Errors handler
	errorReporter, err := reporter.NewImportErrorReporter(errorReporterProducer, log.Namespace)
	if err != nil {
		log.Fatal(ctx, "new import error reporter error", err)
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

	// Start consuming messages from Kafka instanceConsumer
	message.Consume(ctx, instanceConsumer, messageReceiver, cfg)

	instanceConsumer.Channels().LogErrors(ctx, "incoming instance kafka consumer received an error")
	instanceCompleteProducer.Channels().LogErrors(ctx, "completed instance kafka producer received an error")
	errorReporterProducer.Channels().LogErrors(ctx, "error reporter kafka producer received an error")

	// If we receive a signal (SIGINT or SIGTERM), start graceful shutdown
	signal := <-signals
	log.Info(ctx, "os signal received, attempting graceful shutdown", log.Data{"signal": signal.String()})

	shutdownCtx, cancel := context.WithTimeout(ctx, cfg.GracefulShutdownTimeout)
	hasShutdownError := false

	go func() {

		defer cancel() // cancel shutdown context timer

		if serviceList.HealthCheck {
			log.Info(ctx, "stopping health check")
			hc.Stop()
		}

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Error(ctx, "error shutting down http server", err)
			hasShutdownError = true
		}

		if serviceList.InstanceConsumer {
			log.Info(shutdownCtx, "stop listening to instance kafka consumer")
			if err := instanceConsumer.StopListeningToConsumer(shutdownCtx); err != nil {
				log.Error(ctx, "error on stop listening to instance kafka consumer", err)
				hasShutdownError = true
			}
		}

		if serviceList.InstanceConsumer {
			log.Info(shutdownCtx, "closing instance kafka consumer")
			if err := instanceConsumer.Close(shutdownCtx); err != nil {
				log.Error(ctx, "error closing instance kafka consumer", err)
				hasShutdownError = true
			}
		}

		if serviceList.InstanceCompleteProducer {
			log.Info(shutdownCtx, "closing instance complete kafka producer")
			if err := instanceCompleteProducer.Close(shutdownCtx); err != nil {
				log.Error(ctx, "error closing instance complete kafka consumer", err)
				hasShutdownError = true
			}
		}

		if serviceList.GraphDB {
			log.Info(shutdownCtx, "closing graph db")
			if err := graphDB.Close(shutdownCtx); err != nil {
				log.Error(ctx, "error closing graph db", err)
				hasShutdownError = true
			}

			log.Info(shutdownCtx, "closing graph db error consumer")
			if err := graphErrorConsumer.Close(shutdownCtx); err != nil {
				log.Error(ctx, "error closing graph db error consumer", err)
				hasShutdownError = true
			}
		}

		if serviceList.ErrorReporterProducer {
			log.Info(shutdownCtx, "closing error reporter kafka producer")
			if err := errorReporterProducer.Close(shutdownCtx); err != nil {
				log.Error(ctx, "error closing error reporter kafka producer", err)
				hasShutdownError = true
			}
		}
	}()

	// wait for timeout or success (cancel)
	<-shutdownCtx.Done()

	if hasShutdownError {
		err = errors.New("failed to shutdown gracefully")
		log.Error(ctx, "failed to shutdown gracefully ", err)
		os.Exit(1)
	}

	log.Info(ctx, "graceful shutdown complete")
	os.Exit(0)
}

// StartHealthCheck sets up the Handler, starts the healthcheck and the http server that serves health endpoint
func startHealthCheck(ctx context.Context, hc *healthcheck.HealthCheck, bindAddr string) *dphttp.Server {
	router := mux.NewRouter()
	router.Path("/health").HandlerFunc(hc.Handler)
	hc.Start(ctx)

	httpServer := dphttp.NewServer(bindAddr, router)
	httpServer.HandleOSSignals = false

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Error(ctx, "http server error", err)
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
		log.Error(context.Background(), "error adding check for kafka instance consumer checker", err)
	}

	if err = hc.AddCheck("Kafka InstanceComplete Producer", instanceCompleteProducer.Checker); err != nil {
		hasErrors = true
		log.Error(context.Background(), "error adding check for kafka instance complete producer checker", err)
	}

	if err = hc.AddCheck("Kafka ErrorReporter Producer", errorReporterProducer.Checker); err != nil {
		hasErrors = true
		log.Error(context.Background(), "error adding check for kafka error reporter checker", err)
	}

	if err = hc.AddCheck("Dataset", datasetClient.Checker); err != nil {
		hasErrors = true
		log.Error(context.Background(), "error adding check for dataset checker", err)
	}

	if err = hc.AddCheck("Graph DB", db.Checker); err != nil {
		hasErrors = true
		log.Error(context.Background(), "error adding check for graph db", err)
	}

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}
	return nil
}
