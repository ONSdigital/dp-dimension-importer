package client

import (
	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
)

// Neo4j client provides functions running queries and executing statments against a Neo4j database.
type Neo4j struct{}

const (
	stmtKey       = "statement"
	stmtParamsKey = "params"
)

var (
	neoLogger  = logging.Logger{Name: "client.Neo4j"}
	errConnNil = errors.New("connection required but was nil")
)

// Query execute a query against neo. Caller is responsible for closing conn
func (neo Neo4j) Query(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
	if conn == nil {
		return nil, errConnNil
	}
	if len(query) == 0 {
		return nil, errors.New("query required but was empty")
	}
	var err error

	var neoStmt bolt.Stmt
	if neoStmt, err = createStmt(conn, query); err != nil {
		return nil, errors.Wrap(err, "error while attempting to create statement")
	}

	defer neoStmt.Close()

	var rows bolt.Rows
	if rows, err = neoStmt.QueryNeo(params); err != nil {
		return nil, errors.Wrap(err, "error while attempting to execute query")
	}
	defer rows.Close()

	allRows, _, err := rows.All()
	if err != nil {
		return nil, errors.Wrap(err, "while attempting to retrieve query result row data")
	}

	return &common.NeoRows{Data: allRows}, nil
}

// ExecStmt execute a statement against neo. Caller is responsible for closing conn
func (neo Neo4j) ExecStmt(conn bolt.Conn, stmt string, params map[string]interface{}) (bolt.Result, error) {
	if conn == nil {
		return nil, errConnNil
	}
	if len(stmt) == 0 {
		return nil, errors.New("statement required but was empty")
	}

	var err error

	var neoStmt bolt.Stmt
	if neoStmt, err = createStmt(conn, stmt); err != nil {
		return nil, errors.Wrap(err, "error while attempting to create statement")
	}
	defer neoStmt.Close()

	var results bolt.Result
	if results, err = neoStmt.ExecNeo(params); err != nil {
		return nil, errors.Wrap(err, "error while attempting to execute neo statement")
	}
	return results, nil
}

func createStmt(conn bolt.Conn, stmt string) (bolt.Stmt, error) {
	neoStmt, err := conn.PrepareNeo(stmt)
	if err != nil {
		return nil, err
	}
	return neoStmt, nil
}
