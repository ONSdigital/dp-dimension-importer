package client

import (
	"fmt"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"errors"
	"os"
	"strconv"
	"sync"
)

const (
	// Create a unique constraint on the dimension type value.
	uniqueDimConstStmt = "CREATE CONSTRAINT ON (d:%s) ASSERT d.value IS UNIQUE"

	// Create the dimension node and the HAS_DIMENSION relationship to the Instance it belongs to.
	createDimensionAndInstanceRelStmt = "MATCH (i:%s) CREATE (d:%s {value: {value}}) CREATE (i)-[:HAS_DIMENSION]->(d) RETURN ID(d)"

	// Create an Insatnce node.
	createInstanceStmt = "CREATE (i:%s) RETURN i"

	// Update the Instance node with the list of dimension types it contains.
	addInstanceDimensionsStmt = "MATCH (i:%s) SET i.dimensions = {dimensions_list}"

	valueKey                  = "value"
	stmtKey                   = "statment"
	stmtParamsKey             = "params"
	errCreatingStatment       = "Error creating statement."
	errExecutingStatment      = "Error executing statement"
	errReturningRows          = "Error return query rows"
	errDrivePoolInit          = "Unexpected error while attempting to create bolt driver pool: %v"
	insertDimSuccessMsg       = "Dimension sucessfully inserted into graph"
	insertDimValidationMsg    = "InsertDimension: dimension failed validation"
	openConnErrMsg            = "Unexpected error when attempting to open bolt connection: %v"
	dimensionIDRequiredErr    = "dimension.Dimension_ID is required but was empty"
	dimensionValueRequiredErr = "dimension.Value is required but was empty"
	dimensionInvalidErr       = "Dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty"
	dimensionRequiredErr      = "Dimension is required but was nil"
	nodeIDCastErr             = "Unexpected error while casting NodeID to int64"
)

var (
	once          sync.Once
	driverPool    bolt.DriverPool
	neoDriverPool NeoDriverPool
)

type NeoDriverPool interface {
	OpenPool() (bolt.Conn, error)
}

// Neo4j client provides functions for inserting dimension, instances and relationships into a Neo4j database.
type Neo4j struct{}

// InitialiseDatabaseClient creates a new instance of the Neo4j client and does the initial setup required for the
// client to connect to the database instance.
func InitialiseDatabaseClient(url string, poolSize int) Neo4j {
	once.Do(func() {
		//var pool NeoDriverPool
		pool, err := bolt.NewDriverPool(url, poolSize)
		if err != nil {
			logging.Error.Printf(errDrivePoolInit, log.Data{
				logKeys.URL:      url,
				logKeys.PoolSize: poolSize,
			})
			os.Exit(1)
		}
		neoDriverPool = pool
	})
	return Neo4j{}
}

func (neo Neo4j) InsertDimension(i model.Instance, d *model.Dimension) (*model.Dimension, error) {
	logData := log.Data{
		logKeys.DimensionID: d.Dimension_ID,
		valueKey:            d.Value,
	}

	var err error
	if err = validate(d); err != nil {
		log.ErrorC(insertDimValidationMsg, err, logData)
		return nil, err
	}

	params := map[string]interface{}{valueKey: d.Value}
	logData[stmtParamsKey] = params

	var conn bolt.Conn
	if conn, err = neoDriverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return nil, err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	args := []interface{}{i.GetInstanceLabel(), d.GetDimensionLabel()}
	if neoStmt, err = neo.createStmt(conn, logData, createDimensionAndInstanceRelStmt, args); err != nil {
		return nil, err
	}
	defer neoStmt.Close()

	// Insert the dimension node into the graph.
	var rows bolt.Rows
	if rows, err = neoStmt.QueryNeo(params); err != nil {
		log.ErrorC(errExecutingStatment, err, logData)
		return nil, err
	}
	defer rows.Close()

	var data []interface{}
	if data, _, err = rows.NextNeo(); err != nil {
		log.ErrorC(errReturningRows, err, nil)
		return nil, err
	}

	nodeID, ok := data[0].(int64)
	if !ok {
		return nil, errors.New(nodeIDCastErr)
	}
	d.NodeId = strconv.FormatInt(nodeID, 10)
	//logData[logKeys.NodeID] = nodeID
	//log.Debug(insertDimSuccessMsg, logData)
	logging.Debug.Print(insertDimSuccessMsg)
	return d, nil
}

// CreateUniqueConstraint create a unique constrain on the Demension entity name and the value property.
func (neo Neo4j) CreateUniqueConstraint(d *model.Dimension) error {
	if err := validate(d); err != nil {
		logging.Error.Print(dimensionRequiredErr)
		return err
	}

	logDebug := log.Data{
		logKeys.DimensionID: d.GetDimensionLabel(),
		stmtKey:             uniqueDimConstStmt,
	}
	logging.Debug.Printf("Creating unique constraint on dimension: %v", logDebug)

	var err error
	var conn bolt.Conn
	if conn, err = neoDriverPool.OpenPool(); err != nil {
		logging.Error.Printf(openConnErrMsg, err)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, logDebug, uniqueDimConstStmt, []interface{}{d.GetDimensionLabel()}); err != nil {
		logging.Error.Printf("Error creating Neo Statment: %v", err)
		return err
	}
	defer neoStmt.Close()

	if _, err = neoStmt.ExecNeo(nil); err != nil {
		logDebug[logKeys.ErrorDetails] = err.Error()
		logging.Error.Printf("Error executing unique constraint Statment: %v", logDebug)
		return err
	}
	return nil
}

func (neo Neo4j) CreateInstance(instance model.Instance) error {
	logDebug := make(map[string]interface{})
	var err error
	var conn bolt.Conn
	if conn, err = driverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, logDebug, createInstanceStmt, []interface{}{instance.GetInstanceLabel()}); err != nil {
		log.ErrorC("CreateInstance: Failed to create statment.", err, logDebug)
		return err
	}
	defer neoStmt.Close()

	if _, err = neoStmt.ExecNeo(nil); err != nil {
		log.ErrorC("CreateInstance: Failed to create instance.", err, nil)
		return err
	}
	return nil
}

func (neo Neo4j) AddInstanceDimensions(instance model.Instance) error {
	logDebug := make(map[string]interface{})
	var err error
	var conn bolt.Conn
	if conn, err = driverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, logDebug, addInstanceDimensionsStmt, []interface{}{instance.GetInstanceLabel()}); err != nil {
		log.ErrorC("CreateInstance: Failed to create statment.", err, nil)
		return err
	}
	defer neoStmt.Close()

	dimensionNames := make([]interface{}, 0)
	for _, name := range instance.GetDistinctDimensionNames() {
		dimensionNames = append(dimensionNames, name)
	}

	createInstParams := map[string]interface{}{"dimensions_list": dimensionNames}
	if _, err = neoStmt.ExecNeo(createInstParams); err != nil {
		log.ErrorC("CreateInstance: Failed to create instance.", err, nil)
		return err
	}
	return nil
}

func (neo Neo4j) createStmt(conn bolt.Conn, logData map[string]interface{}, stmtFmt string, args []interface{}) (bolt.Stmt, error) {
	stmt := fmt.Sprintf(stmtFmt, args...)
	logData[stmtKey] = stmt

	neoStmt, err := conn.PrepareNeo(stmt)
	if err != nil {
		log.ErrorC(errCreatingStatment, err, log.Data{
			stmtKey: stmt,
		})
		return nil, err
	}
	return neoStmt, nil
}

func validate(d *model.Dimension) error {
	if d == nil {
		return errors.New(dimensionNilErr)
	}
	if len(d.Dimension_ID) == 0 && len(d.Value) == 0 {
		return errors.New(dimensionInvalidErr)
	}
	if len(d.Dimension_ID) == 0 {
		return errors.New(dimensionIDRequiredErr)
	}
	if len(d.Value) == 0 {
		return errors.New(dimensionValueRequiredErr)
	}
	return nil
}
