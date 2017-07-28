package handler

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
)

const eventRecievedMsg = "Handling dimensions extracted event."
const dimensionCliErrMsg = "Error when calling dimensions client"

type ImportAPIClient interface {
	GetDimensions(instanceID string) ([]*model.Dimension, error)
	SetDimensionNodeID(instanceID string, d *model.Dimension) error
}

type DimensionsDatabase interface {
	InsertDimension(dimension *model.Dimension) (*model.Dimension, error)
	CreateUniqueConstraint(d *model.Dimension) error
	CreateInstanceNode(instanceID string, distinctDimensions []*model.Dimension) error
	CreateInstanceDimensionRelationships(instanceID string, distinctDimensions []*model.Dimension) error
}

type DimensionsExtractedEventHandler struct {
	DimensionsStore DimensionsDatabase
	ImportAPI       ImportAPIClient
}

func (hdl *DimensionsExtractedEventHandler) HandleEvent(event model.DimensionsExtractedEvent) {
	logData := log.Data{logKeys.InstanceID: event.InstanceID}

	log.Debug(eventRecievedMsg, logData)

	dimensions, err := hdl.ImportAPI.GetDimensions(event.InstanceID)

	if err != nil {
		log.ErrorC(dimensionCliErrMsg, err, logData)
		panic(err)
	}

	logData[logKeys.DimensionsCount] = len(dimensions)

	// Track the uniqueDimIDs dimension types for this isnstanceID.
	uniqueDimIDs := make(map[string]interface{})
	uniqueDims := make([]*model.Dimension, 0)

	for _, d := range dimensions {
		if _, ok := uniqueDimIDs[d.Dimension_ID]; !ok {
			uniqueDimIDs[d.Dimension_ID] = nil
			uniqueDims = append(uniqueDims, d)

			if err := hdl.DimensionsStore.CreateUniqueConstraint(d); err != nil {
				log.ErrorC("unexected error while attempting to create uniqueDimIDs dimension constaint.", err, nil)
			}
		}

		d, err := hdl.DimensionsStore.InsertDimension(d)
		if err != nil {
			log.Debug("Failed to insert dimension", log.Data{
				logKeys.DimensionID:  d.Dimension_ID,
				logKeys.ErrorDetails: err.Error(),
			})
		}

		if err := hdl.ImportAPI.SetDimensionNodeID(event.InstanceID, d); err != nil {
			log.ErrorC("Unexpected error while updating dimension node_id.", err, nil)
		}
	}

	if err := hdl.DimensionsStore.CreateInstanceNode(event.InstanceID, uniqueDims); err != nil {
		panic(err)
	}

	if err := hdl.DimensionsStore.CreateInstanceDimensionRelationships(event.InstanceID, uniqueDims); err != nil {
		panic(err)
	}
}
