package model

type DimensionsExtractedEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type Dimensions struct {
	InstanceId string `json:"instanceId"`
	Items      []Dimension `json:"items"`
}

type Dimension struct {
	NodeId    string `json:"nodeId,omitempty"`
	NodeNamde string `json:"nodeName"`
	Value     string `json:"value"`
}
