package handler

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"sync"
)

type DimensionsClient interface {
	Get(instanceID string) (*model.Dimensions, error)
}

type DatabaseClient interface {
	BatchInsert(instanceID string, batch []model.Dimension) error
}

type DimensionsExtractedHandler struct {
	DimensionsCli DimensionsClient
	DBCli         DatabaseClient
	BatchSize     int
}

func (h DimensionsExtractedHandler) Handle(event model.DimensionsExtractedEvent) {
	logData := log.Data{common.InstanceIDKey: event.InstanceID}

	log.Debug("Handling dimensions extracted event.", logData)

	dimensions, err := h.DimensionsCli.Get(event.InstanceID)
	if err != nil {
		log.ErrorC("Error when calling dimensions client", err, logData)
		panic(err)
	}

	logData["totalDimensions"] = len(dimensions.Items)
	logData["batchSize"] = h.BatchSize
	log.Debug("Beginning batch processing dimensions.", logData)
	var wg sync.WaitGroup

	h.batchProcess(&wg, event.InstanceID, dimensions.Items)
	log.Debug("batchJobs pending.", nil)
	wg.Wait()
	log.Debug("batchJobs completed.", nil)
}

func (h DimensionsExtractedHandler) batchProcess(wg *sync.WaitGroup, instanceID string, dimensions []model.Dimension) {
	if len(dimensions) == 0 {
		log.Debug("All batches have been submitted for processing.", log.Data{
			common.InstanceIDKey: instanceID,
		})
		return
	}

	if len(dimensions) <= h.BatchSize {
		wg.Add(1)
		go func() {
			if err := h.DBCli.BatchInsert(instanceID, dimensions); err != nil {
				log.ErrorC("Dimemsion batch insert failure", err, log.Data{
					common.InstanceIDKey: instanceID,
				})
			}
			defer wg.Done()
		}()
		return
	}

	wg.Add(1)
	go func() {
		if err := h.DBCli.BatchInsert(instanceID, dimensions[:h.BatchSize]); err != nil {
			log.ErrorC("Batch failed", err, nil)
		}
		defer wg.Done()
	}()
	h.batchProcess(wg, instanceID, dimensions[h.BatchSize:])
}
