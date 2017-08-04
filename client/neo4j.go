package client

import (
	"fmt"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"errors"
	"os"
	"strconv"
	"sync"
)

//go:generate moq -out generated_bolt_mocks.go . NeoConn NeoDriverPool NeoStmt

// NeoConn type to easliy allow MOQ to generate a mock from the bolt.Conn interface
type NeoConn bolt.Conn

// NeoStmt type to easliy allow MOQ to generate a mock from the bolt.Stmt interface
type NeoStmt bolt.Stmt

type NeoDriverPool interface {
	OpenPool() (bolt.Conn, error)
}

// Neo4j client provides functions for inserting dimension, instances and relationships into a Neo4j database.
type Neo4j struct{}

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
	instanceIDReqErr          = "Instance.InstanceID is required but was empty"
)

var (
	once sync.Once
	//driverPool    bolt.DriverPool
	neoDriverPool NeoDriverPool
)

// NewDatabase creates a new instance of the Neo4j client and does the initial setup required for the
// client to connect to the database instance.
func NewDatabase(url string, poolSize int) Neo4j {
	once.Do(func() {
		pool, err := bolt.NewDriverPool(url, poolSize)
		if err != nil {
			log.ErrorC(errDrivePoolInit, err, log.Data{
				logKeys.URL:      url,
				logKeys.PoolSize: poolSize,
			})
			os.Exit(1)
		}
		neoDriverPool = pool
	})
	return Neo4j{}
}

func (neo Neo4j) InsertDimension(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
	logData := log.Data{
		logKeys.DimensionID: dimension.Dimension_ID,
		valueKey:            dimension.Value,
	}

	var err error
	if err = validate(dimension); err != nil {
		log.ErrorC(insertDimValidationMsg, err, logData)
		return nil, err
	}

	params := map[string]interface{}{valueKey: dimension.Value}
	logData[stmtParamsKey] = params

	var conn bolt.Conn
	if conn, err = neoDriverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return nil, err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, logData, createDimensionAndInstanceRelStmt, instance.GetLabel(), dimension.GetLabel()); err != nil {
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
	dimension.NodeId = strconv.FormatInt(nodeID, 10)
	logData[logKeys.NodeID] = nodeID
	log.Debug(insertDimSuccessMsg, logData)

	return dimension, nil
}

// CreateUniqueConstraint create a unique constrain on the Demension entity name and the value property.
func (neo Neo4j) CreateUniqueConstraint(dimension *model.Dimension) error {
	if err := validate(dimension); err != nil {
		log.ErrorC(dimensionRequiredErr, err, nil)
		return err
	}

	logDebug := log.Data{
		logKeys.DimensionID: dimension.GetLabel(),
		stmtKey:             uniqueDimConstStmt,
	}
	log.Debug("Creating unique constraint on dimension", logDebug)

	var err error
	var conn bolt.Conn
	if conn, err = neoDriverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, logDebug, uniqueDimConstStmt, dimension.GetLabel()); err != nil {
		log.ErrorC("Error creating Neo Statment", err, nil)
		return err
	}
	defer neoStmt.Close()

	if _, err = neoStmt.ExecNeo(nil); err != nil {
		logDebug[logKeys.ErrorDetails] = err.Error()
		log.ErrorC("Error executing unique constraint Statment", err, logDebug)
		return err
	}
	return nil
}

func (neo Neo4j) CreateInstance(instance *model.Instance) error {
	if len(instance.InstanceID) == 0 {
		err := errors.New(instanceIDReqErr)
		log.ErrorC(instanceIDReqErr, err, nil)
		return err
	}
	logDebug := make(map[string]interface{})
	var err error
	var conn bolt.Conn
	if conn, err = neoDriverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, logDebug, createInstanceStmt, instance.GetLabel()); err != nil {
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

func (neo Neo4j) AddInstanceDimensions(instance *model.Instance) error {
	logDebug := make(map[string]interface{})
	var err error
	var conn bolt.Conn
	if conn, err = neoDriverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, logDebug, addInstanceDimensionsStmt, instance.GetLabel()); err != nil {
		log.ErrorC("CreateInstance: Failed to create statment.", err, nil)
		return err
	}
	defer neoStmt.Close()

	createInstParams := map[string]interface{}{"dimensions_list": instance.GetDimensions()}
	if _, err = neoStmt.ExecNeo(createInstParams); err != nil {
		log.ErrorC("CreateInstance: Failed to create instance.", err, nil)
		return err
	}
	return nil
}

func (neo Neo4j) createStmt(conn bolt.Conn, logData map[string]interface{}, stmtFmt string, args ... interface{}) (bolt.Stmt, error) {
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
