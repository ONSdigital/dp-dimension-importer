package event

// NewInstance represents a 'Dimensions Extracted' kafka messagae.
type NewInstance struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

// InstanceCompleted represents a 'Dimensions Inserted' kafka message
type InstanceCompleted struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}
