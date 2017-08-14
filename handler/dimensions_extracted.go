package handler

import (
	"errors"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"time"
)

//go:generate moq -out ../mocks/dimensions_extracted_generated_mocks.go -pkg mocks . ImportAPIClient InstanceRepository DimensionRepository

const (
	logEventRecieved        = "Handling dimensions extracted event"
	dimensionCliErrMsg      = "Error when calling dimensions client"
	instanceErrMsg          = "Error when calling the import API to retrieve instance data"
	updateNodeIDErr         = "Unexpected error while calling ImportAPI.SetDimensionNodeID"
	createInstanceErr       = "Unexpected error while attempting to create instance"
	importAPINilErr         = "DimensionsExtractedEventHandler.ImportAPI expected but was nil"
	createDimRepoNilErr     = "DimensionsExtractedEventHandler.NewDimensionInserter expected but was nil"
	instanceRepoNilErr      = "DimensionsExtractedEventHandler.InstanceRepository expected but was nil"
	instanceIDNilErr        = "DimensionsExtractedEvent.InstanceID is required but was nil"
	insertDimErr            = "Error while attempting to insert dimension"
	addInsanceDimsErr       = "InstanceRepository.AddDimensions returned an error"
	eventProcessingComplete = "Event processing complete"
)

// ImportAPIClient defines interface of an Import API client,
type ImportAPIClient interface {
	GetDimensions(instanceID string) ([]*model.Dimension, error)
	PutDimensionNodeID(instanceID string, dimension *model.Dimension) error
	GetInstance(instanceID string) (*model.Instance, error)
}

// InstanceRepository defines an Instance repository
type InstanceRepository interface {
	Create(instance *model.Instance) error
	AddDimensions(instance *model.Instance) error
}

// DimensionRepository defines a Dimensions repository
type DimensionRepository interface {
	Insert(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error)
}

// DimensionsExtractedEventHandler provides functions for handling DimensionsExtractedEvents.
type DimensionsExtractedEventHandler struct {
	NewDimensionInserter func() DimensionRepository
	InstanceRepository   InstanceRepository
	ImportAPI            ImportAPIClient
}

// HandleEvent retrieves the dimensions for specified instanceID from the Import API, creates an MyInstance entity for
// provided instanceID, creates a Dimension entity for each dimension and a relationship to the MyInstance it belongs to
// and makes a PUT request to the Import API with the database ID of each Dimension entity.
func (hdlr *DimensionsExtractedEventHandler) HandleEvent(event model.DimensionsExtractedEvent) error {
	if hdlr.ImportAPI == nil {
		return errors.New(importAPINilErr)
	}
	if hdlr.InstanceRepository == nil {
		return errors.New(instanceRepoNilErr)
	}
	if hdlr.NewDimensionInserter == nil {
		return errors.New(createDimRepoNilErr)
	}
	if len(event.InstanceID) == 0 {
		return errors.New(instanceIDNilErr)
	}

	logData := log.Data{
		logKeys.InstanceID:      event.InstanceID,
		logKeys.DimensionsCount: 0,
	}
	log.Debug(logEventRecieved, logData)

	start := time.Now()
	var dimensions []*model.Dimension
	var err error

	if dimensions, err = hdlr.ImportAPI.GetDimensions(event.InstanceID); err != nil {
		log.ErrorC(dimensionCliErrMsg, err, logData)
		return errors.New(dimensionCliErrMsg)
	}

	logData[logKeys.DimensionsCount] = len(dimensions)

	// retrieve the CSV header from the import API and attach it to the instance node allowing it to be used after import.
	instance, err := hdlr.ImportAPI.GetInstance(event.InstanceID)
	if err != nil {
		log.ErrorC(instanceErrMsg, err, logData)
		return errors.New(instanceErrMsg)
	}

	if err = hdlr.InstanceRepository.Create(instance); err != nil {
		log.ErrorC(createInstanceErr, err, logData)
		return errors.New(createInstanceErr)
	}

	dimensionInserter := hdlr.NewDimensionInserter()

	for _, dimension := range dimensions {
		if dimension, err = dimensionInserter.Insert(instance, dimension); err != nil {
			logData[logKeys.DimensionID] = dimension.DimensionID
			log.ErrorC(insertDimErr, err, nil)
			return err
		}

		if err = hdlr.ImportAPI.PutDimensionNodeID(event.InstanceID, dimension); err != nil {
			log.ErrorC(updateNodeIDErr, err, nil)
			return errors.New(updateNodeIDErr)
		}
	}

	if err = hdlr.InstanceRepository.AddDimensions(instance); err != nil {
		log.ErrorC(addInsanceDimsErr, err, logData)
		return errors.New(addInsanceDimsErr)
	}

	logData[logKeys.ProcessingTime] = time.Since(start).Seconds()
	log.Debug(eventProcessingComplete, logData)
	return nil
}
