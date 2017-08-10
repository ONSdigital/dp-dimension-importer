package common

import (
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

//go:generate moq -out ../mocks/neo4j_generated_mocks.go -pkg mocks . NeoConn NeoDriverPool NeoStmt NeoQueryRows NeoResult

// NeoConn type to easliy allow MOQ to generate a mock from the bolt.Conn interface
type NeoConn bolt.Conn

// NeoStmt type to easliy allow MOQ to generate a mock from the bolt.Stmt interface
type NeoStmt bolt.Stmt

// NeoQueryRows type to easliy allow MOQ to generate a mock from the bolt.Rows interface
type NeoQueryRows bolt.Rows

// NeoResult type to easliy allow MOQ to generate a mock from the bolt.Result interface
type NeoResult bolt.Result

// NeoDriverPool defines interface for bolt driver pool
type NeoDriverPool interface {
	OpenPool() (bolt.Conn, error)
}

// NeoRows type to hold bolt.Rows data. Is necessary as bolt.Rows is automatically closed if you don't do anything with
// them meaning they cannot be passed back to the calling func. Stuct mirrors bolt.Rows and allows row data to be
// copied into an idenical structure which can be passed back keeping a clean separation between neo specific things and
// the business logic.
type NeoRows struct {
	Data [][]interface{}
}
