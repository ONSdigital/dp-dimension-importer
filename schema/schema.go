package schema

import (
	"github.com/ONSdigital/dp-kafka/v2/avro"
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

// NewInstanceSchema avro schema for a newInstance event
var NewInstanceSchema = &avro.Schema{
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

// InstanceCompletedSchema avro schema for a instanceCompleted event
var InstanceCompletedSchema = &avro.Schema{
	Definition: instanceCompleted,
}
