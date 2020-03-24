package initialise

import (
	"context"
	"fmt"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/store"
	"github.com/ONSdigital/dp-graph/v2/graph"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/log.go/log"
)

// ExternalServiceList represents a list of services
type ExternalServiceList struct {
	InstanceConsumer         bool
	InstanceCompleteProducer bool
	ErrorReporterProducer    bool
	GraphDB                  bool
	Consumer                 bool
	HealthCheck              bool
}

// KafkaProducerName represents a type for kafka producer name used by iota constants
type KafkaProducerName int

// Possible names of Kafka Producers
const (
	InstanceComplete = iota
	ErrorReporter
)

var kafkaProducerNames = []string{"InstanceComplete", "ErrorReporter"}

// Values of the kafka producers names
func (k KafkaProducerName) String() string {
	return kafkaProducerNames[k]
}

// GetConsumer returns a kafka consumer, which might not be initialised
func (e *ExternalServiceList) GetConsumer(ctx context.Context, cfg *config.Config) (kafkaConsumer *kafka.ConsumerGroup, err error) {
	cgChannels := kafka.CreateConsumerGroupChannels(true)
	consumer, err := kafka.NewConsumerGroup(
		ctx, cfg.KafkaAddr, cfg.IncomingInstancesTopic, cfg.IncomingInstancesConsumerGroup, kafka.OffsetNewest, true, cgChannels)
	if err != nil {
		log.Event(ctx, "new kafka consumer group returned an error", log.FATAL, log.Error(err), log.Data{
			"brokers":        cfg.KafkaAddr,
			"topic":          cfg.IncomingInstancesTopic,
			"consumer_group": cfg.IncomingInstancesConsumerGroup,
		})
		return nil, err
	}

	e.InstanceConsumer = true
	return consumer, nil
}

// GetProducer returns a kafka producer, which might not be initialised
func (e *ExternalServiceList) GetProducer(ctx context.Context, brokers []string, topic string, name KafkaProducerName) (kafkaProducer *kafka.Producer, err error) {
	pChannels := kafka.CreateProducerChannels()
	producer, err := kafka.NewProducer(ctx, brokers, topic, 0, pChannels)
	if err != nil {
		log.Event(ctx, "new kafka producer returned an error", log.FATAL, log.Error(err), log.Data{"topic": topic})
		return nil, err
	}

	switch {
	case name == InstanceComplete:
		e.InstanceCompleteProducer = true
	case name == ErrorReporter:
		e.ErrorReporterProducer = true
	default:
		err = fmt.Errorf("kafka producer name not recognised: '%s'. valid names: %v", name.String(), kafkaProducerNames)
	}

	return producer, nil
}

// GetGraphDB returns a connection to the graph DB
func (e *ExternalServiceList) GetGraphDB(ctx context.Context) (store.Storer, error) {
	graphDB, err := graph.New(ctx, graph.Subsets{Instance: true, Dimension: true})
	if err != nil {
		log.Event(ctx, "new graph db returned an error", log.FATAL, log.Error(err))
		return nil, err
	}
	e.GraphDB = true

	return graphDB, nil
}

// NewConsumer creates a new InstanceEvent consumer
func (e *ExternalServiceList) NewConsumer(ctx context.Context, consumer kafka.IConsumerGroup, messageReceiver message.Receiver, defaultShutdown time.Duration) message.Consumer {
	e.Consumer = true
	return message.NewConsumer(ctx, consumer, messageReceiver, defaultShutdown)
}

// GetHealthChecker creates a new healthcheck object
func (e *ExternalServiceList) GetHealthChecker(ctx context.Context, buildTime, gitCommit, version string, cfg *config.Config) (*healthcheck.HealthCheck, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		log.Event(ctx, "failed to create versionInfo for healthcheck", log.FATAL, log.Error(err))
		return nil, err
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckRecoveryInterval, cfg.HealthCheckInterval)
	e.HealthCheck = true

	return &hc, nil
}
