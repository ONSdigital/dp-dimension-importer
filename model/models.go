package model

import (
	"errors"
	"fmt"
	"strings"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
	db "github.com/ONSdigital/dp-graph/v2/models"
)

// Dimension struct wraps the Dimension dataset API model defined in dp-api-clients, for extra functionality
type Dimension struct {
	dbDimension *db.Dimension
	codeListID  string
}

// NewDimension creates a new DB wrapped Dimension from an api dimension
func NewDimension(dimension *dataset.Dimension) *Dimension {
	if dimension == nil {
		return &Dimension{&db.Dimension{}, ""}
	}
	dbDimension := &db.Dimension{
		DimensionID: dimension.DimensionID,
		Option:      dimension.Option,
		NodeID:      dimension.NodeID,
	}

	return &Dimension{
		dbDimension: dbDimension,
		codeListID:  dimension.Links.CodeList.ID,
	}
}

// DbModel returns a DB representation of the Dimension model
func (d *Dimension) DbModel() *db.Dimension {
	return d.dbDimension
}

// CodeListID returns the code list ID linked by this dimension
func (d *Dimension) CodeListID() string {
	return d.codeListID
}

// GetName return the name or type of Dimension e.g. sex, geography time etc.
func (d *Dimension) GetName(instanceID string) string {
	instID := fmt.Sprintf("_%s_", instanceID)
	dimLabel := "_" + d.dbDimension.DimensionID
	result := strings.Replace(dimLabel, instID, "", 2)
	return result
}

// Validate validates tha Dimension is not nil and that there are values for DimensionID and Option
func (d *Dimension) Validate() error {
	if d == nil {
		return errors.New("dimension is required but was nil")
	}
	if len(d.dbDimension.DimensionID) == 0 && len(d.dbDimension.Option) == 0 {
		return errors.New("dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty")
	}
	if len(d.dbDimension.DimensionID) == 0 {
		return errors.New("dimension id is required but was empty")
	}
	if len(d.dbDimension.Option) == 0 {
		return errors.New("dimension value is required but was empty")
	}
	return nil
}

// Instance struct to hold instance information by wrapping DB Instance model
type Instance struct {
	dbInstance *db.Instance
}

// NewInstance creates a new Instance struct from an API Instance model
func NewInstance(instance *dataset.Instance) *Instance {
	if instance == nil {
		return &Instance{&db.Instance{}}
	}
	return &Instance{
		dbInstance: &db.Instance{
			InstanceID: instance.ID,
			CSVHeader:  instance.CSVHeader,
		},
	}
}

// AddDimension add a dimension distinct type/name to the instance.
func (i *Instance) AddDimension(d *Dimension) {
	i.dbInstance.Dimensions = append(i.dbInstance.Dimensions, d)
}

// DbModel returns the DB model of an instance struct
func (i *Instance) DbModel() *db.Instance {
	return i.dbInstance
}

// Validate validates that the instance is not nil and has an ID
func (i *Instance) Validate() error {
	if i == nil {
		return errors.New("instance is required but was nil")
	}
	if len(i.dbInstance.InstanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}
	return nil
}
