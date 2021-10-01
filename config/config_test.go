package config

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGet(t *testing.T) {
	Convey("Given a clean environment", t, func() {
		os.Clearenv()
		cfg = nil

		Convey("When the config values are retrieved", func() {
			cfg, err := Get(context.Background())

			Convey("Then there should be no error returned", func() {
				So(err, ShouldBeNil)

				Convey("And values should be set to the expected defaults", func() {
					So(cfg.BindAddr, ShouldEqual, ":23000")
					So(cfg.ServiceAuthToken, ShouldEqual, "Bearer 4424A9F2-B903-40F4-85F1-240107D1AFAF")
					So(cfg.KafkaConfig.Brokers[0], ShouldEqual, "localhost:9092")
					So(cfg.KafkaConfig.BatchSize, ShouldEqual, 1)
					So(cfg.KafkaConfig.NumWorkers, ShouldEqual, 1)
					So(cfg.KafkaConfig.OffsetOldest, ShouldEqual, true)
					So(cfg.KafkaConfig.SecProtocol, ShouldEqual, "")
					So(cfg.KafkaConfig.SecClientKey, ShouldEqual, "")
					So(cfg.KafkaConfig.SecClientCert, ShouldEqual, "")
					So(cfg.KafkaConfig.SecCACerts, ShouldEqual, "")
					So(cfg.KafkaConfig.SecSkipVerify, ShouldEqual, false)
					So(cfg.KafkaConfig.IncomingInstancesTopic, ShouldEqual, "dimensions-extracted")
					So(cfg.KafkaConfig.IncomingInstancesConsumerGroup, ShouldEqual, "dp-dimension-importer")
					So(cfg.KafkaConfig.OutgoingInstancesTopic, ShouldEqual, "dimensions-inserted")
					So(cfg.KafkaConfig.EventReporterTopic, ShouldEqual, "report-events")
					So(cfg.DatasetAPIAddr, ShouldEqual, "http://localhost:22000")
					So(cfg.DatasetAPIMaxWorkers, ShouldEqual, 100)
					So(cfg.DatasetAPIBatchSize, ShouldEqual, 1000)
					So(cfg.GracefulShutdownTimeout, ShouldEqual, 5*time.Second)
					So(cfg.HealthCheckInterval, ShouldEqual, 30*time.Second)
					So(cfg.HealthCheckCriticalTimeout, ShouldEqual, 90*time.Second)
					So(cfg.EnablePatchNodeID, ShouldEqual, true)
				})
			})
		})

		Convey("When configuration is called with an invalid security setting", func() {
			defer os.Clearenv()
			os.Setenv("KAFKA_SEC_PROTO", "ssl")
			cfg, err := Get(context.Background())

			Convey("Then an error should be returned", func() {
				So(cfg, ShouldBeNil)
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errors.New("config validation errors: KAFKA_SEC_PROTO has invalid value"))
			})
		})

		Convey("When more than one config is invalid", func() {
			defer os.Clearenv()
			os.Setenv("SERVICE_AUTH_TOKEN", "")
			os.Setenv("KAFKA_SEC_PROTO", "ssl")

			cfg, err := Get(context.Background())

			Convey("Then an error should be returned", func() {
				So(cfg, ShouldBeNil)
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errors.New("config validation errors: no SERVICE_AUTH_TOKEN given, KAFKA_SEC_PROTO has invalid value"))
			})
		})

	})

	Convey("Given configurations already exist", t, func() {
		cfg = getDefaultConfig()

		Convey("When the config values are retrieved", func() {
			config, err := Get(context.Background())

			Convey("Then exisitng config should be returned", func() {
				So(config, ShouldResemble, cfg)

				Convey("And no error should be returned", func() {
					So(err, ShouldBeNil)
				})
			})
		})
	})
}

func TestString(t *testing.T) {
	Convey("Given the config values", t, func() {
		cfg, err := Get(context.Background())
		So(err, ShouldBeNil)

		Convey("When String is called", func() {
			cfgStr := cfg.String()

			Convey("Then the string format of config should not contain any sensitive configurations", func() {
				So(cfgStr, ShouldNotContainSubstring, "ServiceAuthToken")
				So(cfgStr, ShouldNotContainSubstring, "BindAddr:[localhost:9092]") // KafkaConfig.BindAddr
				So(cfgStr, ShouldNotContainSubstring, "SecClientKey")

				Convey("And should contain all non-sensitive configurations", func() {
					So(cfgStr, ShouldContainSubstring, "BindAddr")
					So(cfgStr, ShouldContainSubstring, "DatasetAPIAddr")
					So(cfgStr, ShouldContainSubstring, "DatasetAPIMaxWorkers")
					So(cfgStr, ShouldContainSubstring, "DatasetAPIBatchSize")
					So(cfgStr, ShouldContainSubstring, "GracefulShutdownTimeout")
					So(cfgStr, ShouldContainSubstring, "HealthCheckInterval")
					So(cfgStr, ShouldContainSubstring, "HealthCheckCriticalTimeout")
					So(cfgStr, ShouldContainSubstring, "EnablePatchNodeID")

					So(cfgStr, ShouldContainSubstring, "KafkaConfig")
					So(cfgStr, ShouldContainSubstring, "Brokers")
					So(cfgStr, ShouldContainSubstring, "BatchSize")
					So(cfgStr, ShouldContainSubstring, "NumWorkers")
					So(cfgStr, ShouldContainSubstring, "Version")
					So(cfgStr, ShouldContainSubstring, "OffsetOldest")
					So(cfgStr, ShouldContainSubstring, "SecProtocol")
					So(cfgStr, ShouldContainSubstring, "SecClientCert")
					So(cfgStr, ShouldContainSubstring, "SecCACerts")
					So(cfgStr, ShouldContainSubstring, "SecSkipVerify")
					So(cfgStr, ShouldContainSubstring, "IncomingInstancesTopic")
					So(cfgStr, ShouldContainSubstring, "IncomingInstancesConsumerGroup")
					So(cfgStr, ShouldContainSubstring, "OutgoingInstancesTopic")
					So(cfgStr, ShouldContainSubstring, "EventReporterTopic")
				})
			})
		})
	})
}
