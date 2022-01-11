package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/kelseyhightower/envconfig"
)

// KafkaTLSProtocolFlag informs service to use TLS protocol for kafka
const KafkaTLSProtocolFlag = "TLS"

// Config struct to hold application configuration.
type Config struct {
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	ServiceAuthToken           string        `envconfig:"SERVICE_AUTH_TOKEN"            json:"-"`
	DatasetAPIAddr             string        `envconfig:"DATASET_API_ADDR"`
	DatasetAPIMaxWorkers       int           `envconfig:"DATASET_API_MAX_WORKERS"` // maximum number of concurrent go-routines requesting items to datast api at the same time
	DatasetAPIBatchSize        int           `envconfig:"DATASET_API_BATCH_SIZE"`  // maximum size of a response by dataset api when requesting items in batches
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	EnablePatchNodeID          bool          `envconfig:"ENABLE_PATCH_NODE_ID"`
	KafkaConfig                KafkaConfig
}

// KafkaConfig contains the config required to connect to Kafka
type KafkaConfig struct {
	Brokers                        []string `envconfig:"KAFKA_ADDR"                           json:"-"`
	BatchSize                      int      `envconfig:"BATCH_SIZE"`        // number of kafka messages that will be batched
	NumWorkers                     int      `envconfig:"KAFKA_NUM_WORKERS"` // maximum number of concurent kafka messages being consumed at the same time
	Version                        string   `envconfig:"KAFKA_VERSION"`
	OffsetOldest                   bool     `envconfig:"KAFKA_OFFSET_OLDEST"`
	SecProtocol                    string   `envconfig:"KAFKA_SEC_PROTO"`
	SecClientKey                   string   `envconfig:"KAFKA_SEC_CLIENT_KEY"                 json:"-"`
	SecClientCert                  string   `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	SecCACerts                     string   `envconfig:"KAFKA_SEC_CA_CERTS"`
	SecSkipVerify                  bool     `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	IncomingInstancesTopic         string   `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	IncomingInstancesConsumerGroup string   `envconfig:"DIMENSIONS_EXTRACTED_CONSUMER_GROUP"`
	OutgoingInstancesTopic         string   `envconfig:"DIMENSIONS_INSERTED_TOPIC"`
	EventReporterTopic             string   `envconfig:"EVENT_REPORTER_TOPIC"`
}

var cfg *Config

func getDefaultConfig() *Config {
	return &Config{
		BindAddr:         ":23000",
		ServiceAuthToken: "4424A9F2-B903-40F4-85F1-240107D1AFAF",
		KafkaConfig: KafkaConfig{
			Brokers:                        []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			BatchSize:                      1, //not all implementations will allow for batching, so set to a safe default
			NumWorkers:                     1,
			Version:                        "1.0.2",
			OffsetOldest:                   true,
			SecProtocol:                    "",
			SecClientKey:                   "",
			SecClientCert:                  "",
			SecCACerts:                     "",
			SecSkipVerify:                  false,
			IncomingInstancesTopic:         "dimensions-extracted",
			IncomingInstancesConsumerGroup: "dp-dimension-importer",
			OutgoingInstancesTopic:         "dimensions-inserted",
			EventReporterTopic:             "report-events",
		},
		DatasetAPIAddr:             "http://localhost:22000",
		DatasetAPIMaxWorkers:       100,
		DatasetAPIBatchSize:        1000,
		GracefulShutdownTimeout:    time.Second * 5,
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		EnablePatchNodeID:          true,
	}
}

// Get configures the application and returns the configuration
func Get(ctx context.Context) (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg := getDefaultConfig()
	if err := envconfig.Process("", cfg); err != nil {
		log.Error(ctx, "failed to populate config with environment variables", err)
		return nil, err
	}

	errs := validateConfig(ctx, cfg)
	if len(errs) != 0 {
		err := fmt.Errorf("config validation errors: %v", strings.Join(errs, ", "))
		log.Error(ctx, "failed on config validation", err)
		return nil, err
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
