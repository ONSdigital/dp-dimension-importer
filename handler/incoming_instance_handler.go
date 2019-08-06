package handler

import (
	"context"
	"sync"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/store"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
)

//go:generate moq -out ../mocks/incoming_instance_generated_mocks.go -pkg mocks . DatasetAPIClient InstanceRepository DimensionRepository ObservationRepository CompletedProducer

var (
	errInstanceExists = errors.New("[handler.InstanceEventHandler] instance already exists")
	errValidationFail = errors.New("[handler.InstanceEventHandler] validation error")
	logger            = logging.Logger{Name: "handler.InstanceEventHandler"}
)

// DatasetAPIClient defines interface of an Dataset API client,
type DatasetAPIClient interface {
	GetDimensions(instanceID string) ([]*model.Dimension, error)
	PutDimensionNodeID(instanceID string, dimension *model.Dimension) error
	GetInstance(instanceID string) (*model.Instance, error)
}

// CompletedProducer Producer kafka messages for instances that have been successfully processed.
type CompletedProducer interface {
	Completed(e event.InstanceCompleted) error
}

// InstanceEventHandler provides functions for handling DimensionsExtractedEvents.
type InstanceEventHandler struct {
	Store         store.Storer
	DatasetAPICli DatasetAPIClient
	Producer      CompletedProducer
	BatchSize     int
}

// Handle retrieves the dimensions for specified instanceID from the Import API, creates an MyInstance entity for
// provided instanceID, creates a Dimension entity for each dimension and a relationship to the MyInstance it belongs to
// and makes a PUT request to the Import API with the database ID of each Dimension entity.
func (hdlr *InstanceEventHandler) Handle(newInstance event.NewInstance) error {

	err := hdlr.validate(newInstance)
	if err != nil {
		return err
	}

	logData := log.Data{"instance_id": newInstance.InstanceID}
	logger.Info("handling new instance event", logData)

	start := time.Now()

	dimensions, err := hdlr.DatasetAPICli.GetDimensions(newInstance.InstanceID)
	if err != nil {
		return errors.Wrap(err, "DatasetAPICli.GetDimensions returned an error")
	}

	logData["dimensions_count"] = len(dimensions)

	// retrieve the CSV header from the dataset API and attach it to the instance node allowing it to be used after import.
	instance, err := hdlr.DatasetAPICli.GetInstance(newInstance.InstanceID)
	if err != nil {
		return errors.Wrap(err, "dataset api client get instance returned an error")
	}

	if err = hdlr.createInstanceNode(instance); err != nil {
		if err == errInstanceExists {
			logger.Info("an instance with this id already exists, ignoring this event", logData)
			return nil // ignoring
		}
		return err
	}

	if err = hdlr.insertDimensions(instance, dimensions); err != nil {
		return err
	}

	if err = hdlr.createObservationConstraint(instance); err != nil {
		return err
	}

	instanceProcessed := event.InstanceCompleted{
		FileURL:    newInstance.FileURL,
		InstanceID: newInstance.InstanceID,
	}

	if err := hdlr.Producer.Completed(instanceProcessed); err != nil {
		return errors.Wrap(err, "Producer.Completed returned an error")
	}

	logger.Info("instance processing completed successfully", log.Data{"processing_time": time.Since(start).Seconds()})
	return nil
}

func (hdlr *InstanceEventHandler) validate(newInstance event.NewInstance) error {

	if hdlr.DatasetAPICli == nil {
		return errors.New("validation error dataset api client required but was nil")
	}
	if hdlr.Store == nil {
		return errors.New("validation error datastore required but was nil")
	}
	if len(newInstance.InstanceID) == 0 {
		return errors.New("validation error instance_id required but was empty")
	}

	return nil
}

func (hdlr *InstanceEventHandler) insertDimensions(instance *model.Instance, dimensions []*model.Dimension) error {

	cache := make(map[string]string)

	var wg sync.WaitGroup
	problem := make(chan error, len(dimensions))
	count := 0

	for _, dimension := range dimensions {
		wg.Add(1)
		count++
		go func(d *model.Dimension) {
			defer wg.Done()

			d, err := hdlr.Store.InsertDimension(context.Background(), cache, instance, d)
			if err != nil {
				problem <- errors.Wrap(err, "error while attempting to insert dimension")
				return
			}

			if err = hdlr.DatasetAPICli.PutDimensionNodeID(instance.InstanceID, d); err != nil {
				problem <- errors.Wrap(err, "DatasetAPICli.PutDimensionNodeID returned an error")
				return
			}

			// todo: remove this temp hack once the time codelist / input data has been fixed.
			if d.DimensionID != "time" {
				if err = hdlr.Store.CreateCodeRelationship(context.Background(), instance, d.Links.CodeList.ID, d.Option); err != nil {
					problem <- errors.Wrap(err, "error attempting to create relationship to code")
					return
				}
			}
		}(dimension)

		if count >= hdlr.BatchSize {
			wg.Wait()
			if len(problem) > 0 {
				return <-problem
			}
			count = 0
		}
	}

	wg.Wait()
	if len(problem) > 0 {
		return <-problem
	}

	if err := hdlr.Store.AddDimensions(context.Background(), instance); err != nil {

		return errors.Wrap(err, "AddDimensions returned an error")
	}

	return nil
}

func (hdlr *InstanceEventHandler) createInstanceNode(instance *model.Instance) error {

	exists, err := hdlr.Store.InstanceExists(context.Background(), instance)
	if err != nil {
		return errors.Wrap(err, "instance exists check returned an error")
	}

	if exists {
		return errInstanceExists
	}

	if err = hdlr.Store.CreateInstance(context.Background(), instance); err != nil {
		return errors.Wrap(err, "create instance returned an error")
	}

	return nil
}

func (hdlr *InstanceEventHandler) createObservationConstraint(instance *model.Instance) error {

	if err := hdlr.Store.CreateInstanceConstraint(context.Background(), instance); err != nil {
		return errors.Wrap(err, "error while attempting to add the unique observation constraint")
	}

	return nil
}
