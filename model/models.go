package model

type DimensionsExtractedEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type Dimensions struct {
	InstanceId string      `json:"instanceId"`
	Items      []Dimension `json:"items"`
}

type Dimension struct {
	NodeId   string `json:"nodeId,omitempty"`
	NodeName string `json:"nodeName"`
	Value    string `json:"value"`
}

type DimensionEntity struct {
	Method string
	To     string
	id     string
	body   map[string]interface{}
}
