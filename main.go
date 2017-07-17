package main

import (
	"net/http"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/ONSdigital/dp-dimension-importer/kafka"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"os"
)

const instanceIDParam = "instanceID"

var kafkaMsg kafka.Message
var incoming chan kafka.Message
var dimensionsCli client.DimensionsClientImpl

func main() {
	cfg, err := config.Load()
	if err != nil {
		os.Exit(1)
	}

	log.Debug("Application configuration", log.Data{
		"": cfg.String(),
	})

	// Create a sample event.
	dimensionEstractedEvent := model.DimensionsExtractedEvent{
		FileURL: "s3://customise-my-data/test.csv",
		InstanceID: "200",
	}
	// Convert sample event into serialised avro.
	bytes, _ := schema.DimensionsExtractedSchema.Marshal(dimensionEstractedEvent)
	kafkaMsg = kafka.Message{Data: bytes}

	incoming = make(chan kafka.Message)

	handler.DimensionsCli = client.DimensionsClientImpl{DimensionsAddr: cfg.ImporterAddr}
	myConsumer := message.KafkaConsumerImpl{}

	go myConsumer.Consume(incoming)

	r := mux.NewRouter()
	r.HandleFunc("/go", myHandler)

	http.Handle("/", r)
	http.ListenAndServe(cfg.BindAddr, nil)
}

func myHandler(w http.ResponseWriter, r *http.Request) {
	incoming <- kafkaMsg
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}