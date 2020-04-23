package handler

import (
	"context"
	"sync"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/store"
	"github.com/ONSdigital/log.go/log"
	"github.com/pkg/errors"
)

//go:generate moq -out ../mocks/incoming_instance_generated_mocks.go -pkg mocks . CompletedProducer

var (
	errInstanceExists = errors.New("[handler.InstanceEventHandler] instance already exists")
	errValidationFail = errors.New("[handler.InstanceEventHandler] validation error")
	packageName       = "handler.InstanceEventHandler"
)

// CompletedProducer Producer kafka messages for instances that have been successfully processed.
type CompletedProducer interface {
	Completed(ctx context.Context, e event.InstanceCompleted) error
}

// InstanceEventHandler provides functions for handling DimensionsExtractedEvents.
type InstanceEventHandler struct {
	Store         store.Storer
	DatasetAPICli *client.DatasetAPI
	Producer      CompletedProducer
	BatchSize     int
}

// Handle retrieves the dimensions for specified instanceID from the Import API, creates an MyInstance entity for
// provided instanceID, creates a Dimension entity for each dimension and a relationship to the MyInstance it belongs to
// and makes a PUT request to the Import API with the database ID of each Dimension entity.
func (hdlr *InstanceEventHandler) Handle(ctx context.Context, newInstance event.NewInstance) error {

	err := hdlr.validate(newInstance)
	if err != nil {
		return err
	}

	logData := log.Data{"instance_id": newInstance.InstanceID, "package": packageName}
	log.Event(ctx, "handling new instance event", log.INFO, logData)

	start := time.Now()

	dimensions, err := hdlr.DatasetAPICli.GetDimensions(ctx, newInstance.InstanceID)
	if err != nil {
		return errors.Wrap(err, "DatasetAPICli.GetDimensions returned an error")
	}

	logData["dimensions_count"] = len(dimensions)

	// retrieve the CSV header from the dataset API and attach it to the instance node allowing it to be used after import.
	instance, err := hdlr.DatasetAPICli.GetInstance(ctx, newInstance.InstanceID)
	if err != nil {
		return errors.Wrap(err, "dataset api client get instance returned an error")
	}

	if err = hdlr.createInstanceNode(ctx, instance); err != nil {
		if err == errInstanceExists {
			log.Event(ctx, "an instance with this id already exists, ignoring this event", log.INFO, logData)
			return nil // ignoring
		}
		return err
	}

	if err = hdlr.insertDimensions(ctx, instance, dimensions); err != nil {
		return err
	}

	if err = hdlr.createObservationConstraint(ctx, instance); err != nil {
		return err
	}

	instanceProcessed := event.InstanceCompleted{
		FileURL:    newInstance.FileURL,
		InstanceID: newInstance.InstanceID,
	}

	if err := hdlr.Producer.Completed(ctx, instanceProcessed); err != nil {
		return errors.Wrap(err, "Producer.Completed returned an error")
	}

	log.Event(ctx, "instance processing completed successfully", log.INFO, log.Data{"package": packageName, "processing_time": time.Since(start).Seconds()})
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

func (hdlr *InstanceEventHandler) insertDimensions(ctx context.Context, instance *model.Instance, dimensions []*model.Dimension) error {

	cache := make(map[string]string)

	var wg sync.WaitGroup
	problem := make(chan error, len(dimensions))
	count := 0

	for _, dimension := range dimensions {
		wg.Add(1)
		count++
		go func(d *model.Dimension) {
			defer wg.Done()

			dbDimension, err := hdlr.Store.InsertDimension(ctx, cache, instance.DbModel().InstanceID, d.DbModel())
			if err != nil {
				err = errors.Wrap(err, "error while attempting to insert dimension")
				log.Event(ctx, err.Error(), log.Error(err), log.Data{"instance_id": instance.DbModel().InstanceID, "dimension_id": dbDimension.DimensionID})
				problem <- err
				return
			}

			if err = hdlr.DatasetAPICli.PutDimensionNodeID(ctx, instance.DbModel().InstanceID, dimension); err != nil {
				err = errors.Wrap(err, "DatasetAPICli.PutDimensionNodeID returned an error")
				log.Event(ctx, err.Error(), log.Error(err), log.Data{"instance_id": instance.DbModel().InstanceID, "dimension_id": dbDimension.DimensionID})
				problem <- err
				return
			}

			// todo: remove this temp hack once the time codelist / input data has been fixed.
			if dbDimension.DimensionID != "time" {
				if err = hdlr.Store.CreateCodeRelationship(context.Background(), instance.DbModel().InstanceID, dimension.CodeListID(), dbDimension.Option); err != nil {
					err = errors.Wrap(err, "error attempting to create relationship to code")
					log.Event(ctx, err.Error(), log.Error(err), log.Data{"instance_id": instance.DbModel().InstanceID, "dimension_id": dbDimension.DimensionID})
					problem <- err
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

	if err := hdlr.Store.AddDimensions(ctx, instance.DbModel().InstanceID, instance.DbModel().Dimensions); err != nil {
		return errors.Wrap(err, "AddDimensions returned an error")
	}

	return nil
}

func (hdlr *InstanceEventHandler) createInstanceNode(ctx context.Context, instance *model.Instance) error {

	exists, err := hdlr.Store.InstanceExists(ctx, instance.DbModel().InstanceID)
	if err != nil {
		return errors.Wrap(err, "instance exists check returned an error")
	}

	if exists {
		return errInstanceExists
	}

	if err = hdlr.Store.CreateInstance(ctx, instance.DbModel().InstanceID, instance.DbModel().CSVHeader); err != nil {
		return errors.Wrap(err, "create instance returned an error")
	}

	return nil
}

func (hdlr *InstanceEventHandler) createObservationConstraint(ctx context.Context, instance *model.Instance) error {

	if err := hdlr.Store.CreateInstanceConstraint(ctx, instance.DbModel().InstanceID); err != nil {
		return errors.Wrap(err, "error while attempting to add the unique observation constraint")
	}

	return nil
}
