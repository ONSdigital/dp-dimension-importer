package client

import (
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"sync"
)

// Neo4j client provides functions for inserting dimension, instances and relationships into a Neo4j database.
type Neo4j struct {
	driverPool common.NeoDriverPool
}

const (
	stmtKey              = "statment"
	stmtParamsKey        = "params"
	openConnErr          = "Error while attempting to open connection"
	errExecutingStatment = "Error while attempting to execute statement"
	errDrivePoolInit     = "Error while attempting to initialize neo4j driver pool"
	errCreatingStatment  = "Error while attempting to create neo4j statement"
	errQueryWasEmpty     = "Query is required but was nil or empty"
	errStmtWasEmpty      = "Statement is required but was nil or empty"
	errRetrievingRows    = "Error while attempting to retrieve row data"
)

var once sync.Once

// NewNeo4j creates a new instance of the Neo4j client and does the initial setup required for the
// client to connect to the database instance.
func NewNeo4j(url string, poolSize int) (*Neo4j, error) {
	var err error
	var neo4j *Neo4j
	var pool common.NeoDriverPool

	once.Do(func() {
		pool, err = bolt.NewDriverPool(url, poolSize)
		if err != nil {
			// TODO sort this out.
			panic(err)
		}
		neo4j = &Neo4j{driverPool: pool}
	})

	if err != nil {
		log.ErrorC(errDrivePoolInit, err, log.Data{
			common.URL:      url,
			common.PoolSize: poolSize,
		})
		return nil, err
	}
	return neo4j, nil
}

// Query execute a query against neo.
func (neo Neo4j) Query(query string, params map[string]interface{}) (*common.NeoRows, error) {
	if len(query) == 0 {
		return nil, errors.New(errQueryWasEmpty)
	}
	logData := map[string]interface{}{
		stmtKey:       query,
		stmtParamsKey: params,
	}
	var conn bolt.Conn
	var err error

	if conn, err = neo.driverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErr, err, nil)
		return nil, err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = createStmt(conn, query); err != nil {
		return nil, err
	}
	defer neoStmt.Close()

	var rows bolt.Rows
	if rows, err = neoStmt.QueryNeo(params); err != nil {
		log.ErrorC(errExecutingStatment, err, logData)
		return nil, err
	}
	defer rows.Close()

	allRows, _, err := rows.All()
	if err != nil {
		return nil, errors.New(errRetrievingRows)
	}

	return &common.NeoRows{Data: allRows}, nil
}

// ExecStmt execute a statement against neo.
func (neo Neo4j) ExecStmt(stmt string, params map[string]interface{}) (bolt.Result, error) {
	if len(stmt) == 0 {
		return nil, errors.New(errStmtWasEmpty)
	}
	logData := map[string]interface{}{
		stmtKey:       stmt,
		stmtParamsKey: params,
	}

	var conn bolt.Conn
	var err error

	if conn, err = neo.driverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErr, err, nil)
		return nil, err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = createStmt(conn, stmt); err != nil {
		return nil, err
	}
	defer neoStmt.Close()

	var results bolt.Result
	if results, err = neoStmt.ExecNeo(params); err != nil {
		log.ErrorC(errExecutingStatment, err, logData)
		return nil, err
	}
	return results, nil
}

func createStmt(conn bolt.Conn, stmt string) (bolt.Stmt, error) {
	neoStmt, err := conn.PrepareNeo(stmt)
	if err != nil {
		log.ErrorC(errCreatingStatment, err, nil)
		return nil, err
	}
	return neoStmt, nil
}
