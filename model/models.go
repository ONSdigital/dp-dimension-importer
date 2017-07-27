package model

type DimensionsExtractedEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type Dimension struct {
	Dimension_ID string `json:"dimension_id"`
	Value        string `json:"value"`
	NodeId       string `json:"node_id,omitempty"`
}

type DimensionEntity struct {
	Method string
	To     string
	id     string
	body   map[string]interface{}
}
