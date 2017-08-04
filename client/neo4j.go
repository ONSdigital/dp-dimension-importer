package client

import (
	"errors"
	"fmt"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"os"
	"strconv"
	"sync"
)

//go:generate moq -out generated_bolt_mocks.go . NeoConn NeoDriverPool NeoStmt

// NeoConn type to easliy allow MOQ to generate a mock from the bolt.Conn interface
type NeoConn bolt.Conn

// NeoStmt type to easliy allow MOQ to generate a mock from the bolt.Stmt interface
type NeoStmt bolt.Stmt

// NeoDriverPool defines interface for bolt driver pool
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

	valueKey                     = "value"
	stmtKey                      = "statment"
	stmtParamsKey                = "params"
	dimensionsKey                = "dimensions"
	dimensionsList               = "dimensions_list"
	errCreatingStatment          = "Error creating statement."
	errExecutingStatment         = "Error executing statement"
	errReturningRows             = "Error return query rows"
	errDrivePoolInit             = "Unexpected error while attempting to create bolt driver pool"
	insertDimSuccessMsg          = "Dimension sucessfully inserted into graph"
	insertDimValidationMsg       = "InsertDimension: dimension failed validation"
	openConnErrMsg               = "Unexpected error when attempting to open bolt connection"
	dimensionIDRequiredErr       = "dimension.Dimension_ID is required but was empty"
	dimensionValueRequiredErr    = "dimension.Value is required but was empty"
	dimensionInvalidErr          = "Dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty"
	dimensionRequiredErr         = "Dimension is required but was nil"
	nodeIDCastErr                = "Unexpected error while casting NodeID to int64"
	instanceIDReqErr             = "Instance.InstanceID is required but was empty"
	createStmtErr                = "Error creating Neo Statment"
	uniqueConstErr               = "Error executing unique constraint Statment"
	uniqueConstSuccess           = "Successfully created unique constraint on dimenion"
	createInstanceStmtErr        = "Error while creating create Instance statement"
	createInstanceExecErr        = "Error while executing to create Instance statement."
	createInstanceSuccess        = "Successfully created Instance node."
	addInstanceDimensionsStmtErr = "Error while creating add instance dimensions statement"
	addInstanceDimensionsExecErr = "Error while executing add instance dimensions statement"
	addInstanceDimensionsSuccess = "Successfully added dimensions to Instance node"
)

var (
	once          sync.Once
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

// InsertDimension insert a dimension into the graph
func (neo Neo4j) InsertDimension(i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	logData := log.Data{
		logKeys.DimensionID: d.DimensionID,
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
	if neoStmt, err = neo.createStmt(conn, logData, createDimensionQuery(i, d)); err != nil {
		return nil, err
	}
	defer neoStmt.Close()

	// Insert the d node into the graph.
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
	d.NodeID = strconv.FormatInt(nodeID, 10)
	//logData[logKeys.NodeID] = nodeID
	//log.Debug(insertDimSuccessMsg, logData)

	return d, nil
}

// CreateUniqueConstraint create a unique constraint on the Dimension entity name and the value property.
func (neo Neo4j) CreateUniqueConstraint(d *model.Dimension) error {
	if err := validate(d); err != nil {
		log.ErrorC(dimensionRequiredErr, err, nil)
		return err
	}

	logDebug := log.Data{
		logKeys.DimensionID: d.GetLabel(),
		stmtKey:             uniqueDimConstStmt,
	}

	var err error
	var conn bolt.Conn
	if conn, err = neoDriverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt

	if neoStmt, err = neo.createStmt(conn, logDebug, dimensionUniqueConstraintQuery(d)); err != nil {
		log.ErrorC(createStmtErr, err, nil)
		return err
	}
	defer neoStmt.Close()

	if _, err = neoStmt.ExecNeo(nil); err != nil {
		logDebug[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(uniqueConstErr, err, logDebug)
		return err
	}

	log.Debug(uniqueConstSuccess, logDebug)
	return nil
}

// CreateInstance create an Instance node in the graph
func (neo Neo4j) CreateInstance(i *model.Instance) error {
	if len(i.InstanceID) == 0 {
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
	if neoStmt, err = neo.createStmt(conn, logDebug, createInstanceQuery(i)); err != nil {
		log.ErrorC(createInstanceStmtErr, err, logDebug)
		return err
	}
	defer neoStmt.Close()

	if _, err = neoStmt.ExecNeo(nil); err != nil {
		log.ErrorC(createInstanceExecErr, err, nil)
		return err
	}

	logDebug[logKeys.InstanceID] = i.InstanceID
	log.Debug(createInstanceSuccess, logDebug)
	return nil
}

// AddInstanceDimensions add a list of the dimensions to the Instance node prooperties.
func (neo Neo4j) AddInstanceDimensions(i *model.Instance) error {
	logDebug := make(map[string]interface{})
	var err error
	var conn bolt.Conn
	if conn, err = neoDriverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, logDebug, setInstanceDimensionsQuery(i)); err != nil {
		log.ErrorC(addInstanceDimensionsStmtErr, err, nil)
		return err
	}
	defer neoStmt.Close()

	createInstParams := map[string]interface{}{dimensionsList: i.GetDimensions()}
	if _, err = neoStmt.ExecNeo(createInstParams); err != nil {
		log.ErrorC(addInstanceDimensionsExecErr, err, nil)
		return err
	}

	logDebug[logKeys.InstanceID] = i.InstanceID
	logDebug[dimensionsKey] = i.GetDimensions()
	log.Debug(addInstanceDimensionsSuccess, logDebug)
	return nil
}

func (neo Neo4j) createStmt(conn bolt.Conn, logData map[string]interface{}, stmt string) (bolt.Stmt, error) {
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

func dimensionUniqueConstraintQuery(d *model.Dimension) string {
	return fmt.Sprintf(uniqueDimConstStmt, d.GetLabel())
}

func createDimensionQuery(i *model.Instance, d *model.Dimension) string {
	return fmt.Sprintf(createDimensionAndInstanceRelStmt, i.GetLabel(), d.GetLabel())
}

func createInstanceQuery(i *model.Instance) string {
	return fmt.Sprintf(createInstanceStmt, i.GetLabel())
}

func setInstanceDimensionsQuery(i *model.Instance) string {
	return fmt.Sprintf(addInstanceDimensionsStmt, i.GetLabel())
}

func validate(d *model.Dimension) error {
	if d == nil {
		return errors.New(dimensionNilErr)
	}
	if len(d.DimensionID) == 0 && len(d.Value) == 0 {
		return errors.New(dimensionInvalidErr)
	}
	if len(d.DimensionID) == 0 {
		return errors.New(dimensionIDRequiredErr)
	}
	if len(d.Value) == 0 {
		return errors.New(dimensionValueRequiredErr)
	}
	return nil
}
