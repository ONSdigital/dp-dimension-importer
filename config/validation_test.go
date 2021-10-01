package config

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestValidateConfig(t *testing.T) {
	Convey("Given most configs are valid", t, func() {
		ctx := context.Background()
		cfg = getDefaultConfig()

		Convey("And all configurations are valid", func() {

			Convey("When validateConfig is called", func() {
				errs := validateConfig(ctx, cfg)

				Convey("Then no errors should be returned", func() {
					So(errs, ShouldBeEmpty)
				})
			})
		})

		Convey("And SERVICE_AUTH_TOKEN is empty", func() {
			cfg.ServiceAuthToken = ""

			Convey("When validateConfig is called", func() {
				errs := validateConfig(ctx, cfg)

				Convey("Then", func() {
					So(errs, ShouldNotBeEmpty)
					So(errs, ShouldResemble, []string{"no SERVICE_AUTH_TOKEN given"})
				})
			})
		})
	})
}

func TestValidateKafkaValues(t *testing.T) {
	Convey("Given most configs are valid", t, func() {
		cfg = getDefaultConfig()

		Convey("And valid kafka configurations are given", func() {

			Convey("When validateKafkaValues is called", func() {
				errs := validateKafkaValues(cfg.KafkaConfig)

				Convey("Then no error messages should be returned", func() {
					So(errs, ShouldBeEmpty)
				})
			})
		})

		Convey("And an empty KAFKA_ADDR is given", func() {
			cfg.KafkaConfig.Brokers = []string{}

			Convey("When validateKafkaValues is called", func() {
				errs := validateKafkaValues(cfg.KafkaConfig)

				Convey("Then an error message should be returned", func() {
					So(errs, ShouldNotBeEmpty)
					So(errs, ShouldResemble, []string{"no KAFKA_ADDR given"})
				})
			})
		})

		Convey("And BATCH_SIZE is less than 1", func() {
			cfg.KafkaConfig.BatchSize = 0

			Convey("When validateKafkaValues is called", func() {
				errs := validateKafkaValues(cfg.KafkaConfig)

				Convey("Then an error message should be returned", func() {
					So(errs, ShouldNotBeEmpty)
					So(errs, ShouldResemble, []string{"BATCH_SIZE is less than 1"})
				})
			})
		})

		Convey("And KAFKA_NUM_WORKERS is less than 1", func() {
			cfg.KafkaConfig.NumWorkers = 0

			Convey("When validateKafkaValues is called", func() {
				errs := validateKafkaValues(cfg.KafkaConfig)

				Convey("Then an error message should be returned", func() {
					So(errs, ShouldNotBeEmpty)
					So(errs, ShouldResemble, []string{"KAFKA_NUM_WORKERS is less than 0"})
				})
			})
		})

		Convey("And an empty KAFKA_VERSION", func() {
			cfg.KafkaConfig.Version = ""

			Convey("When validateKafkaValues is called", func() {
				errs := validateKafkaValues(cfg.KafkaConfig)

				Convey("Then an error message should be returned", func() {
					So(errs, ShouldNotBeEmpty)
					So(errs, ShouldResemble, []string{"no KAFKA_VERSION given"})
				})
			})
		})

		Convey("And an invalid KAFKA_SEC_PROTO", func() {
			cfg.KafkaConfig.SecProtocol = "invalid"

			Convey("When validateKafkaValues is called", func() {
				errs := validateKafkaValues(cfg.KafkaConfig)

				Convey("Then an error message should be returned", func() {
					So(errs, ShouldNotBeEmpty)
					So(errs, ShouldResemble, []string{"KAFKA_SEC_PROTO has invalid value"})
				})
			})
		})

		Convey("And one of KAFKA_SEC_CLIENT_CERT or KAFKA_SEC_CLIENT_KEY has been set", func() {
			cfg.KafkaConfig.SecClientCert = "test"

			Convey("When validateKafkaValues is called", func() {
				errs := validateKafkaValues(cfg.KafkaConfig)

				Convey("Then an error message should be returned", func() {
					So(errs, ShouldNotBeEmpty)
					So(errs, ShouldResemble, []string{"only one of KAFKA_SEC_CLIENT_CERT or KAFKA_SEC_CLIENT_KEY has been set - requires both"})
				})
			})
		})

		Convey("And more than one invalid kafka configuration", func() {
			cfg.KafkaConfig.Version = ""
			cfg.KafkaConfig.SecProtocol = "invalid"

			Convey("When validateKafkaValues is called", func() {
				errs := validateKafkaValues(cfg.KafkaConfig)

				Convey("Then an error message should be returned", func() {
					So(errs, ShouldNotBeEmpty)
					So(errs, ShouldResemble, []string{"no KAFKA_VERSION given", "KAFKA_SEC_PROTO has invalid value"})
				})
			})
		})
	})
}
