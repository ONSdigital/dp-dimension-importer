package config

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/kelseyhightower/envconfig"
)

// Config struct to hold application configuration.
type Config struct {
	BindAddr                       string        `envconfig:"BIND_ADDR"`
	ServiceAuthToken               string        `envconfig:"SERVICE_AUTH_TOKEN"                     json:"-"`
	KafkaAddr                      []string      `envconfig:"KAFKA_ADDR"`
	KafkaVersion                   string        `envconfig:"KAFKA_VERSION"`
	KafkaOffsetOldest              bool          `envconfig:"KAFKA_OFFSET_OLDEST"`
	KafkaNumWorkers                int           `envconfig:"KAFKA_NUM_WORKERS"` // maximum number of concurent kafka messages being consumed at the same time
	BatchSize                      int           `envconfig:"BATCH_SIZE"`        // number of kafka messages that will be batched
	IncomingInstancesTopic         string        `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	IncomingInstancesConsumerGroup string        `envconfig:"DIMENSIONS_EXTRACTED_CONSUMER_GROUP"`
	OutgoingInstancesTopic         string        `envconfig:"DIMENSIONS_INSERTED_TOPIC"`
	EventReporterTopic             string        `envconfig:"EVENT_REPORTER_TOPIC"`
	DatasetAPIAddr                 string        `envconfig:"DATASET_API_ADDR"`
	DatasetAPIMaxWorkers           int           `envconfig:"DATASET_API_MAX_WORKERS"` // maximum number of concurrent go-routines requesting items to datast api at the same time
	DatasetAPIBatchSize            int           `envconfig:"DATASET_API_BATCH_SIZE"`  // maximum size of a response by dataset api when requesting items in batches
	GracefulShutdownTimeout        time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval            time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout     time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	EnablePatchNodeID              bool          `envconfig:"ENABLE_PATCH_NODE_ID"`
}

var cfg *Config

// Get configures the application and returns the configuration
func Get(ctx context.Context) (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg := &Config{
		BindAddr:                       ":23000",
		ServiceAuthToken:               "4424A9F2-B903-40F4-85F1-240107D1AFAF",
		KafkaAddr:                      []string{"localhost:9092"},
		KafkaVersion:                   "1.0.2",
		KafkaOffsetOldest:              true,
		KafkaNumWorkers:                1,
		BatchSize:                      1, // not all implementations will allow for batching, so set to a safe default
		IncomingInstancesTopic:         "dimensions-extracted",
		IncomingInstancesConsumerGroup: "dp-dimension-importer",
		OutgoingInstancesTopic:         "dimensions-inserted",
		EventReporterTopic:             "report-events",
		DatasetAPIAddr:                 "http://localhost:22000",
		DatasetAPIMaxWorkers:           100,
		DatasetAPIBatchSize:            1000,
		GracefulShutdownTimeout:        time.Second * 5,
		HealthCheckInterval:            30 * time.Second,
		HealthCheckCriticalTimeout:     90 * time.Second,
		EnablePatchNodeID:              true,
	}

	if len(cfg.ServiceAuthToken) == 0 {
		err := errors.New("error while attempting to load config. service auth token is required but has not been configured")
		log.Error(ctx, "service auth token error", err)
		return nil, err
	}

	if err := envconfig.Process("", cfg); err != nil {
		return cfg, err
	}

	cfg.ServiceAuthToken = "Bearer " + cfg.ServiceAuthToken

	return cfg, nil
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Config) String() string {
	b, _ := json.Marshal(config)
	return string(b)
}
