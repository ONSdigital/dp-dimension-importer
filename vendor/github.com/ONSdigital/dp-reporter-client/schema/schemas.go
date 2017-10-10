package schema

import "github.com/ONSdigital/go-ns/avro"

var reportEvent = `{
  "type": "record",
  "name": "report-event",
  "fields": [
    {"name": "instance_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "event_message", "type": "string"},
    {"name": "service_name", "type": "string"}
  ]
}`

var ReportEventSchema *avro.Schema = &avro.Schema{
	Definition: reportEvent,
}
