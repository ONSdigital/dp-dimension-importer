package model

import (
	"fmt"
	"strings"
)

const instance_label = "_%s_Instance"

type DimensionsExtractedEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type Dimension struct {
	Dimension_ID string `json:"dimension_id"`
	Value        string `json:"value"`
	NodeId       string `json:"node_id,omitempty"`
}

func (d *Dimension) GetDimensionLabel() string {
	return "_" + d.Dimension_ID
}

func (d *Dimension) GetName(instanceID string) string {
	instID := fmt.Sprintf("_%s_", instanceID)
	return strings.Replace(d.GetDimensionLabel(), instID, "", 2)
}

func NewInstance(instanceID string) *Instance {
	return &Instance{
		instanceID:             instanceID,
		distinctDimensionNames: make([]string, 0),
		uniqueDimensionIDs:     make(map[string]*Dimension),
	}
}

type Instance struct {
	instanceID             string
	distinctDimensionNames []string
	uniqueDimensionIDs     map[string]*Dimension
}

func (i Instance) GetID() string {
	return i.instanceID
}

func (i Instance) GetInstanceLabel() string {
	return fmt.Sprintf(instance_label, i.instanceID)
}

func (i *Instance) IsDimensionDistinct(instanceID string, d *Dimension) bool {
	var exists bool
	if _, exists = i.uniqueDimensionIDs[d.Dimension_ID]; !exists {
		i.uniqueDimensionIDs[d.Dimension_ID] = nil
		i.distinctDimensionNames = append(i.distinctDimensionNames, d.GetName(instanceID))
	}
	return !exists
}

func (i *Instance) GetDistinctDimensionNames() []string {
	return i.distinctDimensionNames
}
