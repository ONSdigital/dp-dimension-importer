package handler

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"time"
	"github.com/ONSdigital/dp-dimension-importer/logging"
)

const logEventRecieved = "Handling dimensions extracted event | %v"
const dimensionCliErrMsg = "Error when calling dimensions client"
const uniqueConstraintErr = "Unexected error while attempting to create unique Dimension ID constaint."
const insertDimErr = "Unexpected error while attempting to insert dimension"
const updateNodeIDErr = "Unexpected error while calling ImportAPI.SetDimensionNodeID"
const createInstanceErr = "Unexpected error while attempting to create instance"

type ImportAPIClient interface {
	GetDimensions(instanceID string) ([]*model.Dimension, error)
	PutDimensionNodeID(instanceID string, d *model.Dimension) error
}

type DimensionsDatabase interface {
	// InsertDimension persist a dimension into the database
	InsertDimension(i model.Instance, d *model.Dimension) (*model.Dimension, error)
	// CreateUniqueConstraint create a unique constrain for this dimension type.
	CreateUniqueConstraint(d *model.Dimension) error
	// CreateInstance create an instance entity in the database.
	CreateInstance(instance model.Instance) error
	// AddInstanceDimensions update the Instance entity with the distinct dimensions types the dataset contains.
	AddInstanceDimensions(instance model.Instance) error
}

// DimensionsExtractedEventHandler provides functions for handling DimensionsExtractedEvents.
type DimensionsExtractedEventHandler struct {
	DimensionsStore DimensionsDatabase
	ImportAPI       ImportAPIClient
	BatchSize       int
}

// HandleEvent retrieves the dimensions for specified instanceID from the Import API, creates an Instance entity for
// provided instanceID, creates a Dimension entity for each dimension and a relationship to the Instance it belongs to
// and makes a PUT request to the Import API with the database ID of each Dimension entity.
func (handler *DimensionsExtractedEventHandler) HandleEvent(event model.DimensionsExtractedEvent) {
	logData := log.Data{logKeys.InstanceID: event.InstanceID}
	logging.Debug.Printf(logEventRecieved, logData)

	start := time.Now()
	dimensions, err := handler.ImportAPI.GetDimensions(event.InstanceID)

	if err != nil {
		log.ErrorC(dimensionCliErrMsg, err, logData)
		panic(err)
	}

	logData[logKeys.DimensionsCount] = len(dimensions)

	instance := model.NewInstance(event.InstanceID)
	if err := handler.DimensionsStore.CreateInstance(*instance); err != nil {
		log.ErrorC(createInstanceErr, err, logData)
		panic(err)
	}

	for _, dimension := range dimensions {
		if instance.IsDimensionDistinct(event.InstanceID, dimension) {
			if err := handler.DimensionsStore.CreateUniqueConstraint(dimension); err != nil {
				log.ErrorC(uniqueConstraintErr, err, nil)
			}
		}

		d, err := handler.DimensionsStore.InsertDimension(*instance, dimension)
		if err != nil {
			log.Debug(insertDimErr, log.Data{
				logKeys.DimensionID:  d.Dimension_ID,
				logKeys.ErrorDetails: err.Error(),
			})
		}

		if err := handler.ImportAPI.PutDimensionNodeID(event.InstanceID, d); err != nil {
			log.ErrorC(updateNodeIDErr, err, nil)
		}
	}

	if err := handler.DimensionsStore.AddInstanceDimensions(*instance); err != nil {
		// TODO ADD LOGGING HERE.
		panic(err)
	}

	logging.Trace.Printf("Event processing complete. %v", log.Data{
		"dimensions": len(dimensions),
		"seconds":    time.Since(start).Seconds(),
	})
}
