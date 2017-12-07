package repository

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
)

//go:generate moq -out ../mocks/repository_generated_mocks.go -pkg mocks . Neo4jClient

var loggerD = logging.Logger{Name: "repository.DimensionRepository"}

const (
	// Create a unique constraint on the dimension type value.
	uniqueDimConstStmt = "CREATE CONSTRAINT ON (d:`%s`) ASSERT d.value IS UNIQUE"

	// Create the dimension node and the HAS_DIMENSION relationship to the Instance it belongs to.
	createDimensionAndInstanceRelStmt = "MATCH (i:`%s`) CREATE (d:`%s` {value: {value}}) CREATE (i)-[:HAS_DIMENSION]->(d) RETURN ID(d)"

	instanceLabelFmt = "_%s_Instance"
)

// Neo4jClient defines a client for executing statements and queries against a neo4j graph database.
type Neo4jClient interface {
	Query(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error)
	ExecStmt(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error)
}

// DimensionRepository provides functionality for inserting Dimensions into a database.
type DimensionRepository struct {
	constraintsCache map[string]string
	neo4jCli         Neo4jClient
	conn             bolt.Conn
}

// NewDimensionRepository returns a new instance using the given neo4j client.
func NewDimensionRepository(connPool common.NeoDriverPool, neo4jCli Neo4jClient) (*DimensionRepository, error) {
	conn, err := connPool.OpenPool()
	if err != nil {
		return nil, errors.Wrap(err, "connPool.OpenPool returned an error")
	}

	return &DimensionRepository{
		neo4jCli:         neo4jCli,
		constraintsCache: make(map[string]string, 0),
		conn:             conn,
	}, nil
}

// Close attempts to close the repository's neo4j connection.
func (repo DimensionRepository) Close() {
	if err := repo.conn.Close(); err != nil {
		loggerD.ErrorC("conn.Close returned an error", err, nil)
		return
	}
	loggerD.Info("conn closed successfully", nil)
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
	dimensionLabel := fmt.Sprintf("_%s_%s", i.InstanceID, d.DimensionID)

	if _, exists := repo.constraintsCache[dimensionLabel]; !exists {

		if err := repo.createUniqueConstraint(i.InstanceID, d); err != nil {
			return nil, err
		}
		repo.constraintsCache[dimensionLabel] = dimensionLabel
		i.AddDimension(d)
	}

	var err error
	if d, err = repo.insertDimension(i, d); err != nil {
		return nil, err
	}
	return d, nil
}

func (repo DimensionRepository) createUniqueConstraint(instanceID string, d *model.Dimension) error {
	logData := map[string]interface{}{}
	dimensionLabel := fmt.Sprintf("_%s_%s", instanceID, d.DimensionID)
	stmt := fmt.Sprintf(uniqueDimConstStmt, dimensionLabel)

	loggerD.Info("executing create unique constraints statement", logData)

	if _, err := repo.neo4jCli.ExecStmt(repo.conn, stmt, nil); err != nil {
		return errors.Wrap(err, "neoClient.ExecStmt returned an error")
	}

	logData["dimension"] = d.DimensionID
	loggerD.Info("successfully created unique constraint on dimension", logData)
	return nil
}

func (repo DimensionRepository) insertDimension(i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	logData := log.Data{
		"dimension_id": d.DimensionID,
		"value":        d.Option,
	}

	var err error
	params := map[string]interface{}{"value": d.Option}
	logData["params"] = params

	instanceLabel := fmt.Sprintf(instanceLabelFmt, i.GetID())
	dimensionLabel := fmt.Sprintf("_%s_%s", i.InstanceID, d.DimensionID)
	stmt := fmt.Sprintf(createDimensionAndInstanceRelStmt, instanceLabel, dimensionLabel)

	logData["statement"] = stmt
	loggerD.Info("executing insert dimension statement", logData)

	var rows *common.NeoRows
	if rows, err = repo.neo4jCli.Query(repo.conn, stmt, params); err != nil {
		return nil, errors.Wrap(err, "neoClient.Query returned an error")
	}

	data := rows.Data[0]
	nodeID, ok := data[0].(int64)
	if !ok {
		return nil, errors.New("unexpected error while casting node id to int64")
	}

	d.NodeID = strconv.FormatInt(nodeID, 10)
	return d, nil
}

func validateInstance(i *model.Instance) error {
	if i == nil {
		return errors.New("instance is required but was nil")
	}
	if len(i.InstanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}
	return nil
}

func validateDimension(d *model.Dimension) error {
	if d == nil {
		return errors.New("dimension is required but was nil")
	}
	if len(d.DimensionID) == 0 && len(d.Option) == 0 {
		return errors.New("dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty")
	}
	if len(d.DimensionID) == 0 {
		return errors.New("dimension id is required but was empty")
	}
	if len(d.Option) == 0 {
		return errors.New("dimension value is required but was empty")
	}
	return nil
}
