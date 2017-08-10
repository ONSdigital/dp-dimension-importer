package repository

import (
	"errors"
	"fmt"
	"strconv"

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
	uniqueConstErr            = "Error executing unique constraint Statment"
	uniqueConstSuccess        = "Successfully created unique constraint on dimension"
	dimensionNilErr           = "Dimension is required but was nil"
	dimensionInvalidErr       = "Dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty"
	dimensionIDRequiredErr    = "dimension.Dimension_ID is required but was empty"
	dimensionValueRequiredErr = "dimension.Value is required but was empty"
	nodeIDCastErr             = "Unexpected error while casting NodeID to int64"
	insertDimValidationMsg    = "InsertDimension: dimension failed validation"
	errExecutingStatment      = "Error executing statement"
	errReturningRows          = "Error return query rows"
	uniqueConstraintErr       = "Unexected error while attempting to create unique Dimension ID constaint."
	insertDimErr              = "Unexpected error while attempting to create dimension"
)

// Neo4jClient defines a client for executing statements and queries against a Neo4j graph database.
type Neo4jClient interface {
	Query(query string, params map[string]interface{}) (*common.NeoRows, error)
	ExecStmt(query string, params map[string]interface{}) (bolt.Result, error)
}

// DimensionRepository provides functionality for inserting Dimensions into a database.
type DimensionRepository struct {
	ConstraintsCache map[string]string
	Neo4jCli         Neo4jClient
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

	if _, exists := repo.ConstraintsCache[d.DimensionID]; !exists {

		if err := repo.createUniqueConstraint(d); err != nil {
			log.ErrorC(uniqueConstraintErr, err, nil)
			return nil, err
		}
		repo.ConstraintsCache[d.DimensionID] = d.DimensionID
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
	logDebug := map[string]interface{}{}
	dimensionLabel := "_" + d.DimensionID
	stmt := fmt.Sprintf(uniqueDimConstStmt, dimensionLabel)

	if _, err := repo.Neo4jCli.ExecStmt(stmt, nil); err != nil {
		logDebug[common.ErrorDetails] = err.Error()
		log.ErrorC(uniqueConstErr, err, logDebug)
		return err
	}

	log.Debug(uniqueConstSuccess, logDebug)
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
	if rows, err = repo.Neo4jCli.Query(fmt.Sprintf(createDimensionAndInstanceRelStmt, instanceLabel, dimensionLabel), params); err != nil {
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
