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

var GetDimensions func(string) (*model.Dimensions, error)

var InsertDimensions func(instanceID string, batch []model.Dimension) error

var BatchSize int

var errChan = make(chan error)

func HandleEvent(event model.DimensionsExtractedEvent) {
	logData := log.Data{common.InstanceIDKey: event.InstanceID}

	log.Debug(eventRecievedMsg, logData)

	dimensions, err := GetDimensions(event.InstanceID)

	if err != nil {
		log.ErrorC(dimensionCliErrMsg, err, logData)
		panic(err)
	}

	logData[totalDimensionsKey] = len(dimensions.Items)
	logData[batchSizeKey] = BatchSize
	log.Debug(batchProcessingStartMsg, logData)

	go func() {
		var wg sync.WaitGroup

		createBatches(&wg, event.InstanceID, dimensions.Items)
		wg.Wait()

		close(errChan)
	}()
	for err := range errChan {
		if err != nil {
			log.Debug("ERROR!", nil)
		}
	}
	log.Debug(batchesCompleteMsg, nil)
}

func createBatches(wg *sync.WaitGroup, instanceID string, dimensions []model.Dimension) {
	if len(dimensions) == 0 {
		log.Debug(noMoreBatchesMsg, log.Data{
			common.InstanceIDKey: instanceID,
		})
		return
	}

	if len(dimensions) <= BatchSize {
		insertBatch(wg, instanceID, dimensions)
		return
	}

	insertBatch(wg, instanceID, dimensions[:BatchSize])
	// recursive call to create the next batch.
	createBatches(wg, instanceID, dimensions[BatchSize:])
}

func insertBatch(wg *sync.WaitGroup, instID string, dims []model.Dimension) {
	wg.Add(1)
	go func() {
		if err := InsertDimensions(instID, dims); err != nil {
			log.ErrorC(batchErrMsg, err, nil)
			errChan <- err
		}
		defer wg.Done()
	}()
}
