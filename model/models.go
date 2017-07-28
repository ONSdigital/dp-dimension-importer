package model

import (
	"fmt"
	"strings"
)

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
