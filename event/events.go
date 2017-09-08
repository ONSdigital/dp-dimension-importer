package event

// NewInstanceEvent represents a 'Dimensions Extracted' kafka messagae.
type NewInstanceEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

// InstanceCompletedEvent represents a 'Dimensions Inserted' kafka message
type InstanceCompletedEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type ErrorEvent struct {
	InstanceID string `avro:"instance_id"`
	EventType  string `avro:"event_type"`
	EventMsg   string `avro:"event_message"`
}