package model

// ReportEvent structure of a report event message.
type ReportEvent struct {
	InstanceID  string `avro:"instance_id"`
	EventType   string `avro:"event_type"`
	EventMsg    string `avro:"event_message"`
	ServiceName string `avro:"service_name"`
}
