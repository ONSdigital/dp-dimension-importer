package client

import (
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

//go:generate moq -out generated_bolt_mocks.go . NeoConn NeoDriverPool NeoStmt

// NeoConn type to easliy allow MOQ to generate a mock from the bolt.Conn interface
type NeoConn bolt.Conn

// NeoStmt type to easliy allow MOQ to generate a mock from the bolt.Stmt interface
type NeoStmt bolt.Stmt
