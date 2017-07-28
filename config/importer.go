package config

import (
	"encoding/json"

	"github.com/ian-kent/gofigure"
)

type Config struct {
	BindAddr                 string   `env:"BIND_ADDR" flag:"bind-addr" flagDesc:"The port to bind to."`
	KafkaAddr                []string `env:"KAFKA_ADDR" flag:"kafka-addr" flagDesc:"The address of Kafka topic for inbound messages."`
	DimensionsExtractedTopic string   `env:"DIMENSIONS_EXTRACTED_TOPIC" flag:"dimensions-extracted-topic" flagDesc:"The Kafka topic supplying dimension messages"`
	ImportAddr               string   `env:"IMPORT_ADDR" flag:"import-addr" flagDesc:"The address of Kafka topic for inbound messages."`
	DatabaseURL              string   `env:"DB_URL" flag:"db-url" flagDesc:"The URL of the dimensions database."`
	PoolSize                 int      `env:"DB_POOL_SIZE" flag:"db-pool-size" flagDesc:"The database connection pool size."`
}

func (c *Config) String() string {
	b, _ := json.Marshal(c)
	return string(b)
}

func Load() (*Config, error) {
	cfg := Config{
		BindAddr:                 ":21000",
		ImportAddr:               "http://localhost:21800",
		DatabaseURL:              "bolt://localhost:7687",
		PoolSize:                 20,
		KafkaAddr:                []string{"localhost:9092"},
		DimensionsExtractedTopic: "dimensions-extracted",
	}

	err := gofigure.Gofigure(&cfg)
	return &cfg, err
}
