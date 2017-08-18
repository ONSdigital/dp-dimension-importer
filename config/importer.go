package config

import (
	"encoding/json"

	"errors"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ian-kent/gofigure"
)

// Config struct to hold application configuration.
type Config struct {
	BindAddr                 string   `env:"BIND_ADDR" flag:"bind-addr" flagDesc:"The port to bind to."`
	KafkaAddr                []string `env:"KAFKA_ADDR" flag:"kafka-addr" flagDesc:"The address of Kafka topic for inbound messages."`
	DimensionsExtractedTopic string   `env:"DIMENSIONS_EXTRACTED_TOPIC" flag:"dimensions-extracted-topic" flagDesc:"The Kafka topic supplying dimension messages"`
	DimensionsInsertedTopic  string   `env:"DIMENSIONS_INSERTED_TOPIC" flag:"dimensions-inserted-topic" flagDesc:"The Kafka topic for imported dimension complete messages"`
	ImportAddr               string   `env:"IMPORT_ADDR" flag:"import-addr" flagDesc:"The address of Kafka topic for inbound messages."`
	ImportAuthToken          string   `env:"IMPORT_AUTH_TOKEN" flag:"import-auth-token" flagDesc:"Authentication token required to make PUT requests to import api."`
	DatabaseURL              string   `env:"DB_URL" flag:"db-url" flagDesc:"The URL of the dimensions database."`
	PoolSize                 int      `env:"DB_POOL_SIZE" flag:"db-pool-size" flagDesc:"The database connection pool size."`
}

func (c *Config) String() string {
	authTokenFound := "NOT FOUND"
	if len(c.ImportAuthToken) > 0 {
		authTokenFound = "FOUND"
	}

	masked := Config(*c)
	masked.ImportAuthToken = authTokenFound

	b, _ := json.Marshal(masked)
	return string(b)
}

// Load load the configuration & apply defaults where necessary
func Load() (*Config, error) {
	cfg := Config{
		BindAddr:                 ":21000",
		ImportAddr:               "http://localhost:21800",
		ImportAuthToken:          "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DatabaseURL:              "bolt://localhost:7687",
		PoolSize:                 20,
		KafkaAddr:                []string{"localhost:9092"},
		DimensionsExtractedTopic: "dimensions-extracted",
		DimensionsInsertedTopic:  "dimensions-inserted",
	}

	err := gofigure.Gofigure(&cfg)

	if len(cfg.ImportAuthToken) == 0 {
		err := errors.New("Error while attempting to load config. ImportAuthToken is required but has not been configured.")
		log.Error(err, nil)
		return nil, err
	}
	return &cfg, err
}
