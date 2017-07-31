package handler

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"time"
)

const eventRecievedMsg = "Handling dimensions extracted event."
const dimensionCliErrMsg = "Error when calling dimensions client"
const uniqueConstraintErr = "Unexected error while attempting to create unique Dimension ID constaint."
const insertDimErr = "Unexpected error while attempting to insert dimension"
const updateNodeIDErr = "Unexpected error while calling ImportAPI.SetDimensionNodeID"
const createInstanceErr = "Unexpected error while attempting to create instance"

type ImportAPIClient interface {
	GetDimensions(instanceID string) ([]*model.Dimension, error)
	SetDimensionNodeID(instanceID string, d *model.Dimension) error
}

type DimensionsDatabase interface {
	// InsertDimension persist a dimension into the database
	InsertDimension(dimension *model.Dimension) (*model.Dimension, error)
	// CreateUniqueConstraint create a unique constrain for this dimension type.
	CreateUniqueConstraint(d *model.Dimension) error
	// CreateInstance create an instance entity in the database.
	CreateInstance(instance model.Instance) error
	// AddInstanceDimensions update the Instance entity with the distinct dimensions types the dataset contains.
	AddInstanceDimensions(instance model.Instance) error
	// RelateDimensionsToInstance create a One-to-Many relationship from the instance entity to each of the dimensions it has.
	RelateDimensionsToInstance(instance model.Instance, dimensions []*model.Dimension) error
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

	log.Debug(eventRecievedMsg, logData)

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

	batches := createBatches(100, dimensions, make([][]*model.Dimension, 0))

	for _, currentBatch := range batches {
		log.Debug("Processing dimensions batch", log.Data{
			"size": len(currentBatch),
		})

		for _, dimension := range currentBatch {

			if instance.IsDimensionDistinct(event.InstanceID, dimension) {
				if err := handler.DimensionsStore.CreateUniqueConstraint(dimension); err != nil {
					log.ErrorC(uniqueConstraintErr, err, nil)
				}
			}

			d, err := handler.DimensionsStore.InsertDimension(dimension)
			if err != nil {
				log.Debug(insertDimErr, log.Data{
					logKeys.DimensionID:  d.Dimension_ID,
					logKeys.ErrorDetails: err.Error(),
				})
			}

			if err := handler.ImportAPI.SetDimensionNodeID(event.InstanceID, d); err != nil {
				log.ErrorC(updateNodeIDErr, err, nil)
			}
		}

		if err := handler.DimensionsStore.RelateDimensionsToInstance(*instance, currentBatch); err != nil {
			panic(err)
		}
	}

	if err := handler.DimensionsStore.AddInstanceDimensions(*instance); err != nil {
		// TODO ADD LOGGING HERE.
		panic(err)
	}

	log.Debug("Total time to process req", log.Data{
		"dimensions": len(dimensions),
		"seconds":    time.Since(start).Seconds(),
	})
}

func createBatches(batchSize int, input []*model.Dimension, batches [][]*model.Dimension) [][]*model.Dimension {
	if len(input) == 0 {
		return batches
	}

	if len(input) <= batchSize {
		batches = append(batches, input)
		return batches
	}

	batches = append(batches, input[:batchSize])
	return createBatches(batchSize, input[batchSize:], batches)
}
