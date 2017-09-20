package client

import (
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

// Neo4j client provides functions running queries and executing statments against a Neo4j database.
type Neo4j struct{}

const (
	stmtKey               = "statement"
	stmtParamsKey         = "params"
	openConnErr           = "error while attempting to open connection"
	errExecutingStatement = "error while attempting to execute statement"
	errCreatingStatement  = "error while attempting to create neo4j statement"
	errQueryWasEmpty      = "query is required but was nil or empty"
	errStmtWasEmpty       = "statement is required but was nil or empty"
	errRetrievingRows     = "error while attempting to retrieve row data"
	errConnNil            = "conn is required but was nil"
)

// Query execute a query against neo. Caller is responsible for closing conn
func (neo Neo4j) Query(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
	if conn == nil {
		return nil, errors.New(errConnNil)
	}
	if len(query) == 0 {
		return nil, errors.New(errQueryWasEmpty)
	}
	logData := map[string]interface{}{
		stmtKey:       query,
		stmtParamsKey: params,
	}
	var err error

	var neoStmt bolt.Stmt
	if neoStmt, err = createStmt(conn, query); err != nil {
		return nil, err
	}
	defer neoStmt.Close()

	var rows bolt.Rows
	if rows, err = neoStmt.QueryNeo(params); err != nil {
		log.ErrorC(errExecutingStatement, err, logData)
		return nil, err
	}
	defer rows.Close()

	allRows, _, err := rows.All()
	if err != nil {
		return nil, errors.New(errRetrievingRows)
	}

	return &common.NeoRows{Data: allRows}, nil
}

// ExecStmt execute a statement against neo. Caller is responsible for closing conn
func (neo Neo4j) ExecStmt(conn bolt.Conn, stmt string, params map[string]interface{}) (bolt.Result, error) {
	if conn == nil {
		return nil, errors.New(errConnNil)
	}
	if len(stmt) == 0 {
		return nil, errors.New(errStmtWasEmpty)
	}
	logData := map[string]interface{}{
		stmtKey:       stmt,
		stmtParamsKey: params,
	}

	var err error

	var neoStmt bolt.Stmt
	if neoStmt, err = createStmt(conn, stmt); err != nil {
		return nil, err
	}
	defer neoStmt.Close()

	var results bolt.Result
	if results, err = neoStmt.ExecNeo(params); err != nil {
		log.ErrorC(errExecutingStatement, err, logData)
		return nil, err
	}
	return results, nil
}

func createStmt(conn bolt.Conn, stmt string) (bolt.Stmt, error) {
	neoStmt, err := conn.PrepareNeo(stmt)
	if err != nil {
		log.ErrorC(errCreatingStatement, err, nil)
		return nil, err
	}
	return neoStmt, nil
}
