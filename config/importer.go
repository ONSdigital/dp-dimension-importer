package config

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ONSdigital/go-ns/log"
	"github.com/kelseyhightower/envconfig"
)

// Config struct to hold application configuration.
type Config struct {
	BindAddr                       string        `envconfig:"BIND_ADDR"`
	ServiceAuthToken               string        `envconfig:"SERVICE_AUTH_TOKEN"                     json:"-"`
	KafkaAddr                      []string      `envconfig:"KAFKA_ADDR"`
	IncomingInstancesTopic         string        `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	IncomingInstancesConsumerGroup string        `envconfig:"DIMENSIONS_EXTRACTED_CONSUMER_GROUP"`
	OutgoingInstancesTopic         string        `envconfig:"DIMENSIONS_INSERTED_TOPIC"`
	EventReporterTopic             string        `envconfig:"EVENT_REPORTER_TOPIC"`
	DatasetAPIAddr                 string        `envconfig:"DATASET_API_ADDR"`
	DatasetAPIAuthToken            string        `envconfig:"DATASET_API_AUTH_TOKEN"                 json:"-"`
	DatabaseURL                    string        `envconfig:"DB_URL"`
	PoolSize                       int           `envconfig:"DB_POOL_SIZE"`
	GracefulShutdownTimeout        time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval            time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
}

var cfg *Config

// Get configures the application and returns the configuration
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg := &Config{
		BindAddr:                       ":23000",
		ServiceAuthToken:               "4424A9F2-B903-40F4-85F1-240107D1AFAF",
		DatasetAPIAddr:                 "http://localhost:22000",
		DatasetAPIAuthToken:            "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DatabaseURL:                    "bolt://localhost:7687",
		PoolSize:                       20,
		KafkaAddr:                      []string{"localhost:9092"},
		IncomingInstancesTopic:         "dimensions-extracted",
		IncomingInstancesConsumerGroup: "dp-dimension-importer",
		OutgoingInstancesTopic:         "dimensions-inserted",
		EventReporterTopic:             "report-events",
		GracefulShutdownTimeout:        time.Second * 5,
		HealthCheckInterval:            time.Minute,
	}

	if len(cfg.ServiceAuthToken) == 0 {
		err := errors.New("error while attempting to load config. service auth token is required but has not been configured")
		log.Error(err, nil)
		return nil, err
	}
	return cfg, envconfig.Process("", cfg)
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Config) String() string {
	json, _ := json.Marshal(config)
	return string(json)
}
