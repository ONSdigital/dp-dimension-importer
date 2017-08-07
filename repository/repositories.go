package repository

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
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
