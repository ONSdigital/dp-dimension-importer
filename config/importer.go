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
	BindAddr               string        `envconfig:"BIND_ADDR"`
	KafkaAddr              []string      `envconfig:"KAFKA_ADDR"`
	IncomingInstancesTopic string        `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	OutgoingInstancesTopic string        `envconfig:"DIMENSIONS_INSERTED_TOPIC"`
	EventReporterTopic     string        `envconfig:"EVENT_REPORTER_TOPIC"`
	DatasetAPIAddr         string        `envconfig:"DATASET_API_ADDR"`
	DatasetAPIAuthToken    string        `envconfig:"DATASET_API_AUTH_TOKEN"`
	DatabaseURL            string        `envconfig:"DB_URL"`
	PoolSize               int           `envconfig:"DB_POOL_SIZE"`
	ShutdownTimeout        time.Duration `envconfig:"SHUTDOWN_TIMEOUT"`
}

func (c *Config) String() string {
	authTokenFound := "NOT FOUND"
	if len(c.DatasetAPIAuthToken) > 0 {
		authTokenFound = "FOUND"
	}

	masked := Config(*c)
	masked.DatasetAPIAuthToken = authTokenFound

	b, _ := json.Marshal(masked)
	return string(b)
}

// Load load the configuration & apply defaults where necessary
func Load() (*Config, error) {
	defaultTimeout, err := time.ParseDuration("5s")
	if err != nil {
		log.ErrorC("error while attempting to parse default timeout from string", err, nil)
		return nil, err
	}

	cfg := Config{
		BindAddr:               ":21000",
		DatasetAPIAddr:         "http://localhost:22000",
		DatasetAPIAuthToken:    "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DatabaseURL:            "bolt://localhost:7687",
		PoolSize:               20,
		KafkaAddr:              []string{"localhost:9092"},
		IncomingInstancesTopic: "dimensions-extracted",
		OutgoingInstancesTopic: "dimensions-inserted",
		EventReporterTopic:     "event-reporter",
		ShutdownTimeout:        defaultTimeout,
	}

	err = envconfig.Process("", &cfg)

	if len(cfg.DatasetAPIAuthToken) == 0 {
		err := errors.New("error while attempting to load config. dataset api auth token is required but has not been configured")
		log.Error(err, nil)
		return nil, err
	}
	return &cfg, err
}
