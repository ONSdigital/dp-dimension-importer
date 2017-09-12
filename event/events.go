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

// Error type for any error that occurs while proccessing an instance.
type Error struct {
	InstanceID string `avro:"instance_id"`
	EventType  string `avro:"event_type"`
	EventMsg   string `avro:"event_message"`
}