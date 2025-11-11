package config

import (
	"context"

	"github.com/ONSdigital/log.go/v2/log"
)

func validateConfig(ctx context.Context, cfg *Config) []string {
	errs := []string{}

	if cfg.ServiceAuthToken == "" {
		errs = append(errs, "no SERVICE_AUTH_TOKEN given")
	}

	kafkaCfgErrs := validateKafkaValues(cfg.KafkaConfig)
	if len(kafkaCfgErrs) != 0 {
		log.Info(ctx, "failed kafka configuration validation")
		errs = append(errs, kafkaCfgErrs...)
	}

	return errs
}

func validateKafkaValues(kafkaConfig KafkaConfig) []string {
	errs := []string{}

	if len(kafkaConfig.Brokers) == 0 {
		errs = append(errs, "no KAFKA_ADDR given")
	}

	if kafkaConfig.BatchSize < 1 {
		errs = append(errs, "BATCH_SIZE is less than 1")
	}

	if kafkaConfig.NumWorkers < 1 {
		errs = append(errs, "KAFKA_NUM_WORKERS is less than 0")
	}

	if kafkaConfig.Version == "" {
		errs = append(errs, "no KAFKA_VERSION given")
	}

	if kafkaConfig.SecProtocol != "" && kafkaConfig.SecProtocol != KafkaTLSProtocolFlag {
		errs = append(errs, "KAFKA_SEC_PROTO has invalid value")
	}

	// isKafkaClientCertSet xor isKafkaClientKeySet
	isKafkaClientCertSet := kafkaConfig.SecClientCert != ""
	isKafkaClientKeySet := kafkaConfig.SecClientKey != ""
	if (isKafkaClientCertSet || isKafkaClientKeySet) && !(isKafkaClientCertSet && isKafkaClientKeySet) {
		errs = append(errs, "only one of KAFKA_SEC_CLIENT_CERT or KAFKA_SEC_CLIENT_KEY has been set - requires both")
	}

	return errs
}
