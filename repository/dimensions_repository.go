package repository

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/ONSdigital/dp-dimension-importer/client"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const (
	// Create a unique constraint on the dimension type value.
	uniqueDimConstStmt = "CREATE CONSTRAINT ON (d:`%s`) ASSERT d.value IS UNIQUE"

	// Create the dimension node and the HAS_DIMENSION relationship to the Instance it belongs to.
	createDimensionAndInstanceRelStmt = "MATCH (i:`%s`) CREATE (d:`%s` {value: {value}}) CREATE (i)-[:HAS_DIMENSION]->(d) RETURN ID(d)"

	stmtKey                   = "statment"
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
)

type NeoDatabase interface {
	Query(query string, params map[string]interface{}) (*client.NeoRows, error)
	ExecStmt(query string, params map[string]interface{}) (bolt.Result, error)
}

type DimensionRepository struct {
	ConstraintsCache map[string]string
	Neo              NeoDatabase
}

func (repo DimensionRepository) Insert(i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	if _, exists := repo.ConstraintsCache[d.DimensionID]; !exists {

		if err := repo.createUniqueConstraint(d); err != nil {
			log.ErrorC(uniqueConstraintErr, err, nil)
			return nil, err
		}
		repo.ConstraintsCache[d.DimensionID] = d.DimensionID
		i.AddDimension(d)
	}

	if d, err := repo.insertDimension(i, d); err != nil {
		log.Debug(insertDimErr, log.Data{
			logKeys.DimensionID:  d.DimensionID,
			logKeys.ErrorDetails: err.Error(),
		})
		return nil, err
	}
	return d, nil
}

func (repo DimensionRepository) createUniqueConstraint(d *model.Dimension) error {
	// validate d.
	logDebug := map[string]interface{}{}

	stmt := fmt.Sprintf(uniqueDimConstStmt, d.GetLabel())
	if _, err := repo.Neo.ExecStmt(stmt, nil); err != nil {
		logDebug[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(uniqueConstErr, err, logDebug)
		return err
	}

	log.Debug(uniqueConstSuccess, logDebug)
	return nil
}

func (repo DimensionRepository) insertDimension(i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
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

	var rows *client.NeoRows
	if rows, err = repo.Neo.Query(fmt.Sprintf(createDimensionAndInstanceRelStmt, i.GetLabel(), d.GetLabel()), params); err != nil {
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
