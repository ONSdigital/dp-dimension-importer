package schema

import (
	"github.com/ONSdigital/go-ns/avro"
)

var newInstance = ` {
	"type": "record",
	"name": "dimensions-extracted",
	"namespace": "",
	"fields": [
		{
			"name": "file_url",
			"type": "string"
		},
		{
			"name": "instance_id",
			"type": "string"
		}
	]
}`

var NewInstanceSchema *avro.Schema = &avro.Schema{
	Definition: newInstance,
}

var instanceCompleted = `{
	"type": "record",
	"name": "dimensions-inserted",
	"namespace": "",
	"fields": [
		{
			"name": "file_url",
			"type": "string"
		},
		{
			"name": "instance_id",
			"type": "string"
		}
	]
}`

var InstanceCompletedSchema *avro.Schema = &avro.Schema{
	Definition: instanceCompleted,
}

var errorEvent = `{
  "type": "record",
  "name": "report-event",
  "fields": [
    {
    	"name": "instance_id",
    	"type": "string"
	 },
    {
    	"name": "event_type",
    	"type": "string"
	 },
    {
    	"name": "event_message",
	 	"type": "string"
	 }
  ]
}`

var ErrorEventSchema *avro.Schema = &avro.Schema{
	Definition: errorEvent,
}
