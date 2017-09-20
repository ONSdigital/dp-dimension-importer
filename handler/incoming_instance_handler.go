package handler

import (
	"errors"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"time"
)

//go:generate moq -out ../mocks/dimensions_extracted_generated_mocks.go -pkg mocks . DatasetAPIClient InstanceRepository DimensionRepository CompletedProducer

const (
	logEventRecieved        = "handling dimensions extracted event"
	dimensionCliErrMsg      = "error when calling dimensions client"
	instanceErrMsg          = "error when calling the dataset api to retrieve instance data"
	updateNodeIDErr         = "unexpected error while calling dataset api set dimension node id endpoint"
	createInstanceErr       = "unexpected error while attempting to call InstanceRepository.Create()"
	datasetAPINilErr        = "dimensions extracted event handler: dataset api expected but was nil"
	createDimRepoNilErr     = "dimensions extracted event handler: new dimension inserter expected but was nil"
	instanceRepoNilErr      = "dimensions extracted event handler: instance repository expected but was nil"
	instanceIDNilErr        = "dimensions extracted event: instance id is required but was nil"
	insertDimErr            = "error while attempting to insert dimension"
	addInsanceDimsErr       = "instance repository: add dimensions returned an error"
	eventProcessingComplete = "event processing complete"
	instanceExists          = "instance already exists, removing existing before continuing"
	removeInstanceErr       = "unexpected error while attempting to remove instance and its dimensions."
	removeInstanceSuccess   = "successfully deleted instance and all of its dimensions and relationships"
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
	Exists(instance *model.Instance) (bool, error)
	Delete(instance *model.Instance) error
	Close()
}

// DimensionRepository defines a Dimensions repository
type DimensionRepository interface {
	Insert(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error)
	Close()
}

// CompletedProducer Producer kafka messages for instances that have been successfully processed.
type CompletedProducer interface {
	Completed(e event.InstanceCompleted) error
}

// InstanceEventHandler provides functions for handling DimensionsExtractedEvents.
type InstanceEventHandler struct {
	NewDimensionInserter  func() (DimensionRepository, error)
	NewInstanceRepository func() (InstanceRepository, error)
	DatasetAPICli         DatasetAPIClient
	Producer              CompletedProducer
}

// Handle retrieves the dimensions for specified instanceID from the Import API, creates an MyInstance entity for
// provided instanceID, creates a Dimension entity for each dimension and a relationship to the MyInstance it belongs to
// and makes a PUT request to the Import API with the database ID of each Dimension entity.
func (hdlr *InstanceEventHandler) Handle(newInstance event.NewInstance) error {
	if hdlr.DatasetAPICli == nil {
		return errors.New(datasetAPINilErr)
	}
	if hdlr.NewInstanceRepository == nil {
		return errors.New(instanceRepoNilErr)
	}
	if hdlr.NewDimensionInserter == nil {
		return errors.New(createDimRepoNilErr)
	}
	if len(newInstance.InstanceID) == 0 {
		return errors.New(instanceIDNilErr)
	}

	logData := log.Data{
		logKeys.InstanceID:      newInstance.InstanceID,
		logKeys.DimensionsCount: 0,
	}
	log.Info(logEventRecieved, logData)

	start := time.Now()
	var dimensions []*model.Dimension
	var err error

	if dimensions, err = hdlr.DatasetAPICli.GetDimensions(newInstance.InstanceID); err != nil {
		log.ErrorC(dimensionCliErrMsg, err, logData)
		return errors.New(dimensionCliErrMsg)
	}

	logData[logKeys.DimensionsCount] = len(dimensions)

	// retrieve the CSV header from the dataset API and attach it to the instance node allowing it to be used after import.
	instance, err := hdlr.DatasetAPICli.GetInstance(newInstance.InstanceID)
	if err != nil {
		log.ErrorC(instanceErrMsg, err, logData)
		return errors.New(instanceErrMsg)
	}

	instanceRepo, err := hdlr.NewInstanceRepository()
	if err != nil {
		return err
	}
	defer instanceRepo.Close()

	var exists bool
	if exists, err = instanceRepo.Exists(instance); err != nil {
		return err
	}

	if exists {
		logData := log.Data{logKeys.InstanceID: instance.InstanceID}
		log.Info(instanceExists, logData)
		if err := instanceRepo.Delete(instance); err != nil {
			log.ErrorC(removeInstanceErr, err, logData)
			return err
		}
	}

	if err = instanceRepo.Create(instance); err != nil {
		log.ErrorC(createInstanceErr, err, logData)
		return errors.New(createInstanceErr)
	}

	dimensionInserter, err := hdlr.NewDimensionInserter()
	if err != nil {
		return err
	}
	defer dimensionInserter.Close()

	for _, dimension := range dimensions {
		if dimension, err = dimensionInserter.Insert(instance, dimension); err != nil {
			log.ErrorC(insertDimErr, err, nil)
			return err
		}

		if err = hdlr.DatasetAPICli.PutDimensionNodeID(newInstance.InstanceID, dimension); err != nil {
			log.ErrorC(updateNodeIDErr, err, nil)
			return errors.New(updateNodeIDErr)
		}
	}

	if err = instanceRepo.AddDimensions(instance); err != nil {
		log.ErrorC(addInsanceDimsErr, err, logData)
		return errors.New(addInsanceDimsErr)
	}

	logData[logKeys.ProcessingTime] = time.Since(start).Seconds()
	log.Info(eventProcessingComplete, logData)

	instanceProcessed := event.InstanceCompleted{
		FileURL:    newInstance.FileURL,
		InstanceID: newInstance.InstanceID,
	}

	if err := hdlr.Producer.Completed(instanceProcessed); err != nil {
		return err
	}

	return nil
}
