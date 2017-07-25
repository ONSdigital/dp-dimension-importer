package main

import (
	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/kafka"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"net/http"
	"os"
)

const InstanceIDParam = "instanceID"

var kafkaMsg kafka.Message
var incoming chan kafka.Message
var dimensionsCli client.DimensionsClient

func main() {
	cfg, err := config.Load()
	if err != nil {
		os.Exit(1)
	}

	log.Debug("Application configuration", log.Data{
		"": cfg.String(),
	})

	incoming = make(chan kafka.Message)

	extractedEventHandler := handler.DimensionsExtractedHandler{
		DimensionsCli: client.DimensionsClient{Host: cfg.ImportAddr},
		DBCli:         client.NeoClientInstance(cfg.DatabaseURL, cfg.PoolSize),
		BatchSize:     cfg.BatchSize,
	}

	myConsumer := message.KafkaConsumerImpl{
		EventHandler: extractedEventHandler,
	}

	go myConsumer.Consume(incoming)

	r := mux.NewRouter()
	r.HandleFunc("/go", myHandler)

	http.Handle("/", r)
	http.ListenAndServe(cfg.BindAddr, nil)
}

func myHandler(w http.ResponseWriter, r *http.Request) {
	incoming <- temp()
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

// Generate manually as temp work around to simulate end to end.
func temp() kafka.Message {
	// Create a sample event.
	dimensionExtractedEvent := model.DimensionsExtractedEvent{
		FileURL:    "s3://customise-my-data/test.csv",
		InstanceID: "200",
	}
	// Convert sample event into serialised avro.
	bytes, _ := schema.DimensionsExtractedSchema.Marshal(dimensionExtractedEvent)
	return kafka.Message{Data: bytes}
}
