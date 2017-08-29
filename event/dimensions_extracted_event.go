package event

// DimensionsExtractedEvent represents a 'Dimensions Extracted' kafka messagae.
type DimensionsExtractedEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}
