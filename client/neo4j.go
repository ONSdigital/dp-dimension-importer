package client

import (
	"fmt"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"os"
	"sync"
	"strconv"
)

// Queries & statements.
const uniqueDimConstStmt = "CREATE CONSTRAINT ON (d:%s) ASSERT d.value IS UNIQUE"
const insertDimensionStmt = "CREATE (d: %s {value: {value}}) RETURN ID(d)"
const createInstanceStmt = "CREATE (i:_%s_Instance {dimensions: {dimensions_list}})"
const instanceDimensionRelStmt = "MATCH (i:_%s_Instance) MATCH (d:%s) CREATE (i)-[r:HAS_DIMENSION]->(d)"

const valueKey = "value"
const stmtKey = "statment"
const stmtParamsKey = "params"
const nodeIDKey = "nodeId"

const errCreatingStatment = "Error creating statement."
const errExecutingStatment = "Error executing statement"
const errReturningRows = "Error return query rows"
const errDrivePoolInit = "Unexpected error while attempting to create bolt driver pool"

const insertDimSuccessMsg = "Dimension sucessfully inserted into graph"
const insertDimValidationMsg = "InsertDimension: dimension failed validation"

var errDimensionIDRequired = errors.New("dimension.Dimension_ID is required but was empty")
var errDimensionValueRequired = errors.New("dimension.Value is required but was empty")
var errDimensionEmpty = errors.New("dimenion invalid: both dimension.dimension_id and dimension.value are required but were both empty")
var errDimensionNil = errors.New("dimension is required but was nil")
var errCastingNodeID = errors.New("Unexpected error while casting NodeID")

const openConnErrMsg = "Unexpected error when attempting to open bolt connection"

var once sync.Once
var driverPool bolt.DriverPool

// Neo4j client provides functions for inserting dimension, instances and relationships into a Neo4j database.
type Neo4j struct{}

// InitialiseDatabaseClient creates a new instance of the Neo4j client and does the initial setup required for the
// client to connect to the database instance.
func InitialiseDatabaseClient(url string, poolSize int) Neo4j {
	once.Do(func() {
		pool, err := bolt.NewDriverPool(url, poolSize)
		if err != nil {
			log.ErrorC(errDrivePoolInit, err, log.Data{
				"url":      url,
				"poolSize": poolSize,
			})
			os.Exit(1)
		}
		driverPool = pool
	})
	return Neo4j{}
}

// InsertDimension insert a dimension node in to the graph.
func (neo Neo4j) InsertDimension(d *model.Dimension) (*model.Dimension, error) {
	logData := log.Data{
		logKeys.DimensionID: d.Dimension_ID,
		valueKey:            d.Value,
	}

	var err error
	if err = validate(d); err != nil {
		log.ErrorC(insertDimValidationMsg, err, logData)
		return nil, err
	}

	createStmt := fmt.Sprintf(insertDimensionStmt, d.GetDimensionLabel())
	params := map[string]interface{}{valueKey: d.Value}

	logData[stmtKey] = createStmt
	logData[stmtParamsKey] = params

	var conn bolt.Conn
	if conn, err = driverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return nil, err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, createStmt); err != nil {
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
		return nil, errCastingNodeID
	}
	d.NodeId = strconv.FormatInt(nodeID, 10)
	logData[logKeys.NodeID] = nodeID
	log.Debug(insertDimSuccessMsg, logData)
	return d, nil
}

func (neo Neo4j) CreateUniqueConstraint(d *model.Dimension) error {
	logDebug := log.Data{
		logKeys.DimensionID: d.GetDimensionLabel(),
		stmtKey:             uniqueDimConstStmt,
	}
	log.Debug("Creating unique constraint on dimension", logDebug)

	var err error
	var conn bolt.Conn
	if conn, err = driverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, fmt.Sprintf(uniqueDimConstStmt, d.GetDimensionLabel())); err != nil {
		log.Debug("Error creating Neo Statment", nil)
		return err
	}
	defer neoStmt.Close()

	if _, err = neoStmt.ExecNeo(nil); err != nil {
		log.ErrorC("Dimension not unique", err, logDebug)
	}
	return nil
}

func (neo Neo4j) CreateInstanceNode(instanceID string, distinctDimensions []*model.Dimension) error {
	var err error
	var conn bolt.Conn
	if conn, err = driverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	var neoStmt bolt.Stmt
	if neoStmt, err = neo.createStmt(conn, fmt.Sprintf(createInstanceStmt, instanceID)); err != nil {
		log.ErrorC("CreateInstanceNode: Failed to create statment.", err, nil)
		return err
	}
	defer neoStmt.Close()

	dimNames := make([]interface{}, 0)
	for _, d := range distinctDimensions {
		dimNames = append(dimNames, d.GetName(instanceID))
	}

	createInstParams := map[string]interface{}{"dimensions_list": dimNames}
	if _, err = neoStmt.ExecNeo(createInstParams); err != nil {
		log.ErrorC("CreateInstanceNode: Failed to create instance.", err, nil)
		return err
	}
	return nil
}

func (neo Neo4j) CreateInstanceDimensionRelationships(instanceID string, distinctDimensions []*model.Dimension) error {
	var err error
	var conn bolt.Conn
	if conn, err = driverPool.OpenPool(); err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}
	defer conn.Close()

	relStmts := make([]string, 0)
	for _, d := range distinctDimensions {
		relStmts = append(relStmts, fmt.Sprintf(instanceDimensionRelStmt, instanceID, d.GetDimensionLabel()))
	}

	var pipelineStmt bolt.PipelineStmt
	if pipelineStmt, err = conn.PreparePipeline(relStmts...); err != nil {
		log.ErrorC("CreateInstanceDimensionRelationships: Failed to create statment", err, nil)
		return err
	}
	defer pipelineStmt.Close()

	params := make([]map[string]interface{}, len(relStmts))
	if _, err = pipelineStmt.ExecPipeline(params...); err != nil {
		log.ErrorC("CreateInstanceDimensionRelationships: Failed to exectute statement", err, nil)
		return err
	}
	return nil
}

func (neo Neo4j) createStmt(conn bolt.Conn, stmt string) (bolt.Stmt, error) {
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
		return errDimensionNil
	}
	if len(d.Dimension_ID) == 0 && len(d.Value) == 0 {
		return errDimensionEmpty
	}
	if len(d.Dimension_ID) == 0 {
		return errDimensionIDRequired
	}
	if len(d.Value) == 0 {
		return errDimensionValueRequired
	}
	return nil
}
