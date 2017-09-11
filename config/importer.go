package config

import (
	"encoding/json"
	"time"
)

// Config struct to hold application configuration.
type Config struct {
	BindAddr               string   `envconfig:"BIND_ADDR"`
	KafkaAddr              []string `envconfig:"KAFKA_ADDR"`
	IncomingInstancesTopic string   `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	OutgoingInstancesTopic string   `envconfig:"DIMENSIONS_INSERTED_TOPIC"`
	EventReporterTopic     string   `envconfig:"EVENT_REPORTER_TOPIC"`
	ImportAddr             string   `envconfig:"IMPORT_ADDR"`
	ImportAuthToken        string   `envconfig:"IMPORT_AUTH_TOKEN"`
	DatabaseURL            string   `envconfig:"DB_URL"`
	PoolSize               int      `envconfig:"DB_POOL_SIZE"`
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
	cfg := Config{
		BindAddr:               ":21000",
		ImportAddr:             "http://localhost:22000",
		ImportAuthToken:        "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DatabaseURL:            "bolt://localhost:7687",
		PoolSize:               20,
		KafkaAddr:              []string{"localhost:9092"},
		IncomingInstancesTopic: "dimensions-extracted",
		OutgoingInstancesTopic: "dimensions-inserted",
		EventReporterTopic:     "event-reporter",
		ShutdownTimeout:        time.Duration(5),
	}

	err := envconfig.Process("", &cfg)

	if len(cfg.DatasetAPIAuthToken) == 0 {
		err := errors.New("error while attempting to load config. dataset api auth token is required but has not been configured")
		log.Error(err, nil)
		return nil, err
	}
	return &cfg, err
}
