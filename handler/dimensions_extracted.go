package handler

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"time"
	"errors"
)

//go:generate moq -out generated_mocks.go . ImportAPIClient InstanceRepository DimensionRepository

const (
	logEventRecieved    = "Handling dimensions extracted event"
	dimensionCliErrMsg  = "Error when calling dimensions client"
	updateNodeIDErr     = "Unexpected error while calling ImportAPI.SetDimensionNodeID"
	createInstanceErr   = "Unexpected error while attempting to create instance"
	importAPINilErr     = "DimensionsExtractedEventHandler.ImportAPI expected but was nil"
	createDimRepoNilErr = "DimensionsExtractedEventHandler.CreateDimensionRepository expected but was nil"
	InstanceRepoNilErr  = "DimensionsExtractedEventHandler.InstanceRepository expected but was nil"
	instanceIDNilErr    = "DimensionsExtractedEvent.InstanceID is required but was nil"
	insertDimErr        = "Error while attempting to insert dimension"
	addInsanceDimsErr   = "InstanceRepository.AddDimensions returned an error"
)

type ImportAPIClient interface {
	GetDimensions(instanceID string) ([]*model.Dimension, error)
	PutDimensionNodeID(instanceID string, dimension *model.Dimension) error
}

type InstanceRepository interface {
	Create(instance *model.Instance) error
	AddDimensions(instance *model.Instance) error
}

type DimensionRepository interface {
	Insert(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error)
}

// DimensionsExtractedEventHandler provides functions for handling DimensionsExtractedEvents.
type DimensionsExtractedEventHandler struct {
	CreateDimensionRepository func() DimensionRepository
	InstanceRepository        InstanceRepository
	ImportAPI                 ImportAPIClient
}

// HandleEvent retrieves the dimensions for specified instanceID from the Import API, creates an MyInstance entity for
// provided instanceID, creates a Dimension entity for each dimension and a relationship to the MyInstance it belongs to
// and makes a PUT request to the Import API with the database ID of each Dimension entity.
func (hdlr *DimensionsExtractedEventHandler) HandleEvent(event model.DimensionsExtractedEvent) error {
	if hdlr.ImportAPI == nil {
		return errors.New(importAPINilErr)
	}
	if hdlr.InstanceRepository == nil {
		return errors.New(InstanceRepoNilErr)
	}
	if hdlr.CreateDimensionRepository == nil {
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
	dimensions, err := hdlr.ImportAPI.GetDimensions(event.InstanceID)
	if err != nil {
		log.ErrorC(dimensionCliErrMsg, err, logData)
		return errors.New(dimensionCliErrMsg)
	}

	logData[logKeys.DimensionsCount] = len(dimensions)

	instance := &model.Instance{InstanceID: event.InstanceID, Dimensions: make([]interface{}, 0)}
	if err := hdlr.InstanceRepository.Create(instance); err != nil {
		log.ErrorC(createInstanceErr, err, logData)
		return errors.New(createInstanceErr)
	}

	dimensionRepository := hdlr.CreateDimensionRepository()
	for _, dimension := range dimensions {
		if dimension, err = dimensionRepository.Insert(instance, dimension); err != nil {
			logData[logKeys.DimensionID] = dimension.Dimension_ID
			log.ErrorC(insertDimErr, err, nil)
			return err
		}

		if err := hdlr.ImportAPI.PutDimensionNodeID(event.InstanceID, dimension); err != nil {
			log.ErrorC(updateNodeIDErr, err, nil)
			return errors.New(updateNodeIDErr)
		}
	}

	if err := hdlr.InstanceRepository.AddDimensions(instance); err != nil {
		log.ErrorC(addInsanceDimsErr, err, logData)
		return errors.New(addInsanceDimsErr)
	}

	logData["timeTaken"] = time.Since(start).Seconds()
	log.Debug("Event processing complete", logData)
	return nil
}
