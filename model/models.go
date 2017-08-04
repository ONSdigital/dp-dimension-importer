package model

import (
	"fmt"
	"strings"
)

const instanceLabelFmt = "_%s_Instance"

type DimensionsExtractedEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type Dimension struct {
	Dimension_ID string `json:"dimension_id"`
	Value        string `json:"value"`
	NodeId       string `json:"node_id,omitempty"`
}

func (d *Dimension) GetLabel() string {
	return "_" + d.Dimension_ID
}

func (d *Dimension) GetName(instanceID string) string {
	instID := fmt.Sprintf("_%s_", instanceID)
	dimLabel := d.GetLabel()
	result := strings.Replace(dimLabel, instID, "", 2)
	return result
}

type Instance struct {
	InstanceID string
	Dimensions []interface{}
}

func (i *Instance) GetID() string {
	return i.InstanceID
}

func (i *Instance) AddDimension(d *Dimension) {
	i.Dimensions = append(i.Dimensions, d.GetName(i.InstanceID))
}

func (i *Instance) GetDimensions() []interface{} {
	return i.Dimensions
}

func (i *Instance) GetLabel() string {
	return fmt.Sprintf(instanceLabelFmt, i.GetID())
}