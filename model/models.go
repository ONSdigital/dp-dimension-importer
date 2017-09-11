package model

import (
	"fmt"
	"strings"
)

// DimensionNodeResults wraps dimension node objects for pagination
type DimensionNodeResults struct {
	Items []*Dimension `json:"items"`
}

// Dimension struct encapsulating Dimension details.
type Dimension struct {
	DimensionID string `json:"dimension_id"`
	Value       string `json:"value"`
	NodeID      string `json:"node_id,omitempty"`
}

// GetName return the name or type of Dimension e.g. sex, geography time etc.
func (d *Dimension) GetName(instanceID string) string {
	instID := fmt.Sprintf("_%s_", instanceID)
	dimLabel := "_" + d.DimensionID
	result := strings.Replace(dimLabel, instID, "", 2)
	return result
}

// Instance struct to hold instance information.
type Instance struct {
	InstanceID string   `json:"id,omitempty"`
	CSVHeader  []string `json:"headers"`
	Dimensions []interface{}
}

// GetID return the InstanceID
func (i *Instance) GetID() string {
	return i.InstanceID
}

// AddDimension add a dimension distinct type/name to the instance.
func (i *Instance) AddDimension(d *Dimension) {
	i.Dimensions = append(i.Dimensions, d.GetName(i.InstanceID))
}

// GetDimensions returns a slice of distinct dimensions name/types for this instance.
func (i *Instance) GetDimensions() []interface{} {
	return i.Dimensions
}
