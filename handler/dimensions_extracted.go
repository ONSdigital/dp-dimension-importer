package handler

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"fmt"
	"encoding/json"
)

type DimensionsClient interface {
	Get(instanceID string) (*model.Dimensions, error)
}

var DimensionsCli DimensionsClient

type DimensionsExtractedHandler struct {
	DimensionsCli DimensionsClient
}

func (h DimensionsExtractedHandler) Handle(event model.DimensionsExtractedEvent) {
	fmt.Println("Inside handler.")
	dimensions, err := DimensionsCli.Get(event.InstanceID)

	if err != nil {
		panic(err)
	}

	json, _ := json.Marshal(dimensions)
	fmt.Println(string(json))
}
