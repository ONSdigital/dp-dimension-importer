package common

import "github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"

const InstanceIDKey = "instanceID"
const DimensionsKey = "dimensions"

// errors
var ErrInstanceIDRequired = errors.New("instanceID is required but is empty")
