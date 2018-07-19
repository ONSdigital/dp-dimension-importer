package handler

import (
	"time"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/dp-dimension-importer/model"
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

// InstanceRepository defines an Instance repository
type InstanceRepository interface {
	Create(instance *model.Instance) error
	AddDimensions(instance *model.Instance) error
	CreateCodeRelationship(i *model.Instance, codeListId, code string) error
	Exists(instance *model.Instance) (bool, error)
	Close()
}

// DimensionRepository defines a Dimensions repository
type DimensionRepository interface {
	Insert(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error)
	Close()
}

// ObservationRepository provides storage for observations.
type ObservationRepository interface {
	CreateConstraint(instance *model.Instance) error
	Close()
}

// CompletedProducer Producer kafka messages for instances that have been successfully processed.
type CompletedProducer interface {
	Completed(e event.InstanceCompleted) error
}

// InstanceEventHandler provides functions for handling DimensionsExtractedEvents.
type InstanceEventHandler struct {
	NewDimensionInserter     func() (DimensionRepository, error)
	NewInstanceRepository    func() (InstanceRepository, error)
	NewObservationRepository func() (ObservationRepository, error)
	DatasetAPICli            DatasetAPIClient
	Producer                 CompletedProducer
}

// Handle retrieves the dimensions for specified instanceID from the Import API, creates an MyInstance entity for
// provided instanceID, creates a Dimension entity for each dimension and a relationship to the MyInstance it belongs to
// and makes a PUT request to the Import API with the database ID of each Dimension entity.
func (hdlr *InstanceEventHandler) Handle(newInstance event.NewInstance) error {

	err := hdlr.validate(newInstance)
	if err != nil {
		return err
	}

	logData := log.Data{
		"instance_id": newInstance.InstanceID,
	}
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

	instanceRepo, err := hdlr.NewInstanceRepository()
	if err != nil {
		return errors.Wrap(err, "instance repository constructor returned an error")
	}
	defer instanceRepo.Close()

	err = hdlr.createInstanceNode(instance, instanceRepo)

	if err != nil {
		if err == errInstanceExists {
			logger.Info("an instance with this id already exists, ignoring this event", logData)
			return nil // ignoring
		}
		return err
	}

	err = hdlr.insertDimensions(instance, instanceRepo, dimensions)
	if err != nil {
		return err
	}

	err = hdlr.createObservationConstraint(instance)
	if err != nil {
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
	if hdlr.NewInstanceRepository == nil {
		return errors.New("validation error new instance repository func required but was nil")
	}
	if hdlr.NewDimensionInserter == nil {
		return errors.New("validation error new dimension inserter func required but was nil")
	}
	if len(newInstance.InstanceID) == 0 {
		return errors.New("validation error instance_id required but was empty")
	}

	return nil
}

func (hdlr *InstanceEventHandler) insertDimensions(instance *model.Instance, instanceRepo InstanceRepository, dimensions []*model.Dimension) error {

	dimensionInserter, err := hdlr.NewDimensionInserter()
	if err != nil {
		return errors.Wrap(err, "NewDimensionInserter returned an error")
	}
	defer dimensionInserter.Close()

	for _, dimension := range dimensions {

		dimension, err = dimensionInserter.Insert(instance, dimension)
		if err != nil {
			return errors.Wrap(err, "error while attempting to insert dimension")
		}

		if err = hdlr.DatasetAPICli.PutDimensionNodeID(instance.InstanceID, dimension); err != nil {
			return errors.Wrap(err, "DatasetAPICli.PutDimensionNodeID returned an error")
		}

		// todo: remove this temp hack once the time codelist / input data has been fixed.
		if dimension.DimensionID != "time" {
			if err = instanceRepo.CreateCodeRelationship(instance, dimension.Links.CodeList.ID, dimension.Option); err != nil {
				return errors.Wrap(err, "error attempting to create relationship to code")
			}
		}
	}

	if err = instanceRepo.AddDimensions(instance); err != nil {
		return errors.Wrap(err, "instanceRepo.AddDimensions returned an error")
	}

	return nil
}

func (hdlr *InstanceEventHandler) createInstanceNode(instance *model.Instance, instanceRepo InstanceRepository) error {

	exists, err := instanceRepo.Exists(instance)
	if err != nil {
		return errors.Wrap(err, "instance repository exists check returned an error")
	}

	if exists {
		return errInstanceExists
	}

	if err = instanceRepo.Create(instance); err != nil {
		return errors.Wrap(err, "instanceRepo.Create returned an error")
	}

	return nil
}

func (hdlr *InstanceEventHandler) createObservationConstraint(instance *model.Instance) error {

	observationRepository, err := hdlr.NewObservationRepository()
	if err != nil {
		return errors.Wrap(err, "observation repository constructor returned an error")
	}
	defer observationRepository.Close()

	err = observationRepository.CreateConstraint(instance)
	if err != nil {
		return errors.Wrap(err, "error while attempting to add the unique observation constraint")
	}

	return nil
}
