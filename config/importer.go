package config

import (
	"github.com/ian-kent/gofigure"
	"encoding/json"
)

type Config struct {
	BindAddr     string `env:"BIND_ADDR" flag:"bind-addr" flagDesc:"The port to bind to."`
	KafkaAddr    string `env:"KAFKA_ADDR" flag:"kafka-addr" flagDesc:"The address of Kafka topic for inbound messages."`
	ImporterAddr string `env:"Import_ADDR" flag:"import-addr" flagDesc:"The address of Kafka topic for inbound messages."`
}

func (c *Config) String() string {
	b, _ := json.Marshal(c)
	return string(b)
}

func Load() (*Config, error) {
	cfg := Config{
		BindAddr: ":21000",
		ImporterAddr: ":22000",
	}

	err := gofigure.Gofigure(&cfg)
	return &cfg, err
}