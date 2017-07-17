package schema

import (
	"github.com/ONSdigital/go-ns/avro"
)

var DimensionsExtracted = ` {
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

var DimensionsExtractedSchema *avro.Schema = &avro.Schema{
	Definition: DimensionsExtracted,
}