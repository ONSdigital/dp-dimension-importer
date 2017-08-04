package repository

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
)

const uniqueConstraintErr = "Unexected error while attempting to create unique Dimension ID constaint."
const insertDimErr = "Unexpected error while attempting to create dimension"

type Database interface {
	// InsertDimension persist a dimension into the Database
	InsertDimension(i *model.Instance, d *model.Dimension) (*model.Dimension, error)
	// CreateUniqueConstraint Create a unique constrain for this dimension type.
	CreateUniqueConstraint(d *model.Dimension) error
	// Create Create an instance entity in the Database.
	CreateInstance(instance *model.Instance) error
	// AddDimensions update the MyInstance entity with the distinct dimensions types the dataset contains.
	AddInstanceDimensions(instance *model.Instance) error
}

type InstanceRepository struct {
	Database Database
}

func (repo *InstanceRepository) Create(instance *model.Instance) error {
	return repo.Database.CreateInstance(instance)
}

func (repo *InstanceRepository) AddDimensions(instance *model.Instance) error {
	return repo.Database.AddInstanceDimensions(instance)
}

type DimensionRepository struct {
	ConstraintsCache map[string]string
	Database         Database
}

func (repo DimensionRepository) Insert(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
	if _, exists := repo.ConstraintsCache[dimension.Dimension_ID]; !exists {
		if err := repo.Database.CreateUniqueConstraint(dimension); err != nil {
			log.ErrorC(uniqueConstraintErr, err, nil)
			return nil, err
		}
		repo.ConstraintsCache[dimension.Dimension_ID] = dimension.Dimension_ID
		instance.AddDimension(dimension)
	}

	dimension, err := repo.Database.InsertDimension(instance, dimension)
	if err != nil {
		log.Debug(insertDimErr, log.Data{
			logKeys.DimensionID:  dimension.Dimension_ID,
			logKeys.ErrorDetails: err.Error(),
		})
		return nil, err
	}
	return dimension, nil
}
