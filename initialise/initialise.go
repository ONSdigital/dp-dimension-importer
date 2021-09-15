package initialise

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/store"

	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-graph/v2/graph"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/v2/log"
)

// ExternalServiceList represents a list of services
type ExternalServiceList struct {
	InstanceConsumer         bool
	InstanceCompleteProducer bool
	ErrorReporterProducer    bool
	GraphDB                  bool
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
func (e *ExternalServiceList) GetConsumer(ctx context.Context, kafkaConfig config.KafkaConfig) (kafkaConsumer *kafka.ConsumerGroup, err error) {
	cgChannels := kafka.CreateConsumerGroupChannels(1)
	kafkaOffset := kafka.OffsetNewest
	if kafkaConfig.OffsetOldest {
		kafkaOffset = kafka.OffsetOldest
	}
	cgConfig := &kafka.ConsumerGroupConfig{
		Offset:       &kafkaOffset,
		KafkaVersion: &kafkaConfig.Version,
	}
	consumer, err := kafka.NewConsumerGroup(
		ctx, kafkaConfig.BindAddr, kafkaConfig.IncomingInstancesTopic, kafkaConfig.IncomingInstancesConsumerGroup, cgChannels, cgConfig)

	if err != nil {
		log.Fatal(ctx, "new kafka consumer group returned an error", err, log.Data{
			"brokers":        kafkaConfig.BindAddr,
			"topic":          kafkaConfig.IncomingInstancesTopic,
			"consumer_group": kafkaConfig.IncomingInstancesConsumerGroup,
		})
		return nil, err
	}

	e.InstanceConsumer = true
	return consumer, nil
}

// GetProducer returns a kafka producer, which might not be initialised
func (e *ExternalServiceList) GetProducer(ctx context.Context, topic string, name KafkaProducerName, kafkaConfig config.KafkaConfig) (kafkaProducer *kafka.Producer, err error) {
	pChannels := kafka.CreateProducerChannels()
	pConfig := &kafka.ProducerConfig{
		KafkaVersion: &kafkaConfig.Version,
	}
	producer, err := kafka.NewProducer(ctx, kafkaConfig.BindAddr, topic, pChannels, pConfig)
	if err != nil {
		log.Fatal(ctx, "new kafka producer returned an error", err, log.Data{"topic": topic})
		return nil, err
	}

	switch {
	case name == InstanceComplete:
		e.InstanceCompleteProducer = true
	case name == ErrorReporter:
		e.ErrorReporterProducer = true
	default:
		return producer, fmt.Errorf("kafka producer name not recognised: '%s'. valid names: %v", name.String(), kafkaProducerNames)
	}

	return producer, nil
}

// GetGraphDB returns a connection to the graph DB
func (e *ExternalServiceList) GetGraphDB(ctx context.Context) (store.Storer, error) {
	graphDB, err := graph.New(ctx, graph.Subsets{Instance: true, Dimension: true, CodeList: true})
	if err != nil {
		log.Fatal(ctx, "new graph db returned an error", err)
		return nil, err
	}
	e.GraphDB = true

	return graphDB, nil
}

// GetHealthChecker creates a new healthcheck object
func (e *ExternalServiceList) GetHealthChecker(ctx context.Context, buildTime, gitCommit, version string, cfg *config.Config) (*healthcheck.HealthCheck, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		log.Fatal(ctx, "failed to create versionInfo for healthcheck", err)
		return nil, err
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckCriticalTimeout, cfg.HealthCheckInterval)
	e.HealthCheck = true

	return &hc, nil
}
