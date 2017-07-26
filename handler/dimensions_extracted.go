package handler

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"sync"
)

const eventRecievedMsg = "Handling dimensions extracted event."
const dimensionCliErrMsg = "Error when calling dimensions client"
const batchProcessingStartMsg = "Beginning batch processing dimensions."
const batchesCompleteMsg = "All batches have been processed."
const noMoreBatchesMsg = "All batches have been submitted for processing."
const batchErrMsg = "Batch encountered an error while attempting to process."

// log data keys.
const totalDimensionsKey = "totalDimensions"
const batchSizeKey = "batchSize"

type ImportAPIClient interface {
	GetDimensions(instanceID string) ([]*model.Dimension, error)
	UpdateNodeID(instanceID string, d *model.Dimension) error
}

type DimensionsDatabase interface {
	InsertDimension(instanceID string, dimension *model.Dimension) (*model.Dimension, error)
}

type DimensionsExtractedEventHandler struct {
	DimensionsStore DimensionsDatabase
	ImportAPI       ImportAPIClient
}

func (h *DimensionsExtractedEventHandler) HandleEvent(event model.DimensionsExtractedEvent) {
	logData := log.Data{common.InstanceIDKey: event.InstanceID}

	log.Debug(eventRecievedMsg, logData)

	dimensions, err := h.ImportAPI.GetDimensions(event.InstanceID)

	if err != nil {
		log.ErrorC(dimensionCliErrMsg, err, logData)
		panic(err)
	}

	logData[totalDimensionsKey] = len(dimensions)

	var wg sync.WaitGroup
	wg.Add(len(dimensions))

	for _, d := range dimensions {
		d, err := h.DimensionsStore.InsertDimension(event.InstanceID, d)
		if err != nil {
			log.Debug("Failed to insert dimension", log.Data{
				"Dimension_ID": d.Dimension_ID,
				"details":      err.Error(),
			})
		}
		go func() {
			h.ImportAPI.UpdateNodeID(event.InstanceID, d)
			wg.Done()
		}()
	}
	wg.Wait()
}
