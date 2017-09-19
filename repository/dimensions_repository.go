package repository

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const (
	// Create a unique constraint on the dimension type value.
	uniqueDimConstStmt = "CREATE CONSTRAINT ON (d:`%s`) ASSERT d.value IS UNIQUE"

	// Create the dimension node and the HAS_DIMENSION relationship to the Instance it belongs to.
	createDimensionAndInstanceRelStmt = "MATCH (i:`%s`) CREATE (d:`%s` {value: {value}}) CREATE (i)-[:HAS_DIMENSION]->(d) RETURN ID(d)"

	instanceLabelFmt          = "_%s_Instance"
	stmtKey                   = "statement"
	stmtParamsKey             = "params"
	valueKey                  = "value"
	dimensionsKey             = "dimensions"
	dimensionsList            = "dimensions_list"
	uniqueConstErr            = "error executing unique constraint statement"
	uniqueConstSuccess        = "successfully created unique constraint on dimension"
	dimensionNilErr           = "dimension is required but was nil"
	dimensionInvalidErr       = "dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty"
	dimensionIDRequiredErr    = "dimension id is required but was empty"
	dimensionValueRequiredErr = "dimension value is required but was empty"
	nodeIDCastErr             = "unexpected error while casting node id to int64"
	errExecutingStatment      = "error executing statement"
	uniqueConstraintErr       = "unexpected error while attempting to create unique dimension id constraint"
	insertDimErr              = "unexpected error while attempting to create dimension"
	dimensionkey              = "dimension"
)

// Neo4jClient defines a client for executing statements and queries against a Neo4j graph database.
type Neo4jClient interface {
	Query(query string, params map[string]interface{}) (*common.NeoRows, error)
	ExecStmt(query string, params map[string]interface{}) (bolt.Result, error)
}

// DimensionRepository provides functionality for inserting Dimensions into a database.
type DimensionRepository struct {
	constraintsCache map[string]string
	neo4jCli         Neo4jClient
}

// NewDimensionRepository returns a new instance using the given neo4j client.
func NewDimensionRepository(neo4jCli *client.Neo4j, constraintsCache map[string]string) *DimensionRepository {
	return &DimensionRepository{
		neo4jCli:         neo4jCli,
		constraintsCache: constraintsCache,
	}
}

// Insert inster a dimension into the database and create a unique constrainton the dimension label & value if one
// does not already exist.
func (repo DimensionRepository) Insert(i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	if err := validateInstance(i); err != nil {
		return nil, err
	}
	if err := validateDimension(d); err != nil {
		return nil, err
	}

	if _, exists := repo.constraintsCache[d.DimensionID]; !exists {

		if err := repo.createUniqueConstraint(d); err != nil {
			log.ErrorC(uniqueConstraintErr, err, nil)
			return nil, err
		}
		repo.constraintsCache[d.DimensionID] = d.DimensionID
		i.AddDimension(d)
	}

	if d, err := repo.insertDimension(i, d); err != nil {
		logData := log.Data{
			common.ErrorDetails: err.Error(),
		}
		if d != nil && len(d.DimensionID) > 0 {
			logData[common.DimensionID] = d.DimensionID
		}
		log.Debug(insertDimErr, logData)
		return nil, err
	}
	return d, nil
}

func (repo DimensionRepository) createUniqueConstraint(d *model.Dimension) error {
	logData := map[string]interface{}{}
	dimensionLabel := "_" + d.DimensionID
	stmt := fmt.Sprintf(uniqueDimConstStmt, dimensionLabel)

	if _, err := repo.neo4jCli.ExecStmt(stmt, nil); err != nil {
		logData[common.ErrorDetails] = err.Error()
		log.ErrorC(uniqueConstErr, err, logData)
		return err
	}

	logData[dimensionkey] = d.DimensionID
	log.Debug(uniqueConstSuccess, logData)
	return nil
}

func (repo DimensionRepository) insertDimension(i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	logData := log.Data{
		common.DimensionID: d.DimensionID,
		valueKey:           d.Value,
	}

	var err error
	params := map[string]interface{}{valueKey: d.Value}
	logData[stmtParamsKey] = params

	instanceLabel := fmt.Sprintf(instanceLabelFmt, i.GetID())
	dimensionLabel := "_" + d.DimensionID

	var rows *common.NeoRows
	if rows, err = repo.neo4jCli.Query(fmt.Sprintf(createDimensionAndInstanceRelStmt, instanceLabel, dimensionLabel), params); err != nil {
		log.ErrorC(errExecutingStatment, err, logData)
		return nil, err
	}

	data := rows.Data[0]
	nodeID, ok := data[0].(int64)
	if !ok {
		return nil, errors.New(nodeIDCastErr)
	}

	d.NodeID = strconv.FormatInt(nodeID, 10)
	return d, nil
}

func validateInstance(i *model.Instance) error {
	if i == nil {
		return errors.New(instanceNilErr)
	}
	if len(i.InstanceID) == 0 {
		return errors.New(instanceIDReqErr)
	}
	return nil
}

func validateDimension(d *model.Dimension) error {
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
