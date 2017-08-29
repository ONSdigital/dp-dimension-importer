package event

// DimensionsInsertedEvent represents a 'Dimensions Inserted' kafka message
type DimensionsInsertedEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}
