package repository

import (
	"fmt"

	"github.com/pkg/errors"

	"strings"

	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

var loggerI = logging.Logger{"repository.InstanceRepository"}

const (
	// Create an Insatnce node.
	createInstanceStmt = "CREATE (i:`%s` { header:'%s'}) RETURN i"

	// Count the instances with this ID.
	countInstanceStmt = "MATCH (i: `%s`) RETURN COUNT(*)"

	// Delete an intsance node & all of the dimensions relating to it.
	removeInstanceDimensionsAndRelationships = "MATCH (n)<-[:HAS_DIMENSION]-(i:`%s`) DETACH DELETE n, i"

	// Update the Instance node with the list of dimension types it contains.
	addInstanceDimensionsStmt = "MATCH (i:`%s`) SET i.dimensions = {dimensions_list}"
)

// NewInstanceRepository creates a new InstanceRepository. A bolt.Conn will be obtained from the supplied connectionPool.
// The obtained bolt.Conn will be used for the life time of the InstanceRepository struct
// - it is the responsibility of the caller to call Close when they have finished.
func NewInstanceRepository(connPool common.NeoDriverPool, neo Neo4jClient) (*InstanceRepository, error) {
	conn, err := connPool.OpenPool()
	if err != nil {
		return nil, errors.Wrap(err, "connPool.OpenPool returned an error")
	}
	return &InstanceRepository{neo4j: neo, conn: conn}, nil
}

// InstanceRepository provides functionality for insterting & updating Instances into a neo4j graph database
type InstanceRepository struct {
	neo4j Neo4jClient
	conn  bolt.Conn
}

// Close - closes an open resourecs held by the InstanceRepository.
func (repo *InstanceRepository) Close() {
	if repo.conn != nil {
		if err := repo.conn.Close(); err != nil {
			loggerI.ErrorC("conn.Close returned an error", err, nil)
		}
	}
	loggerI.Info("conn closed successfully", nil)
}

// Create creates an Instance node in a neo4j graph database
func (repo *InstanceRepository) Create(i *model.Instance) error {
	var err error

	if i == nil {
		return errors.New("instance is required but was nil")
	}
	if len(i.InstanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}

	instanceLabel := fmt.Sprintf(instanceLabelFmt, i.GetID())
	createStmt := fmt.Sprintf(createInstanceStmt, instanceLabel, strings.Join(i.CSVHeader, ","))

	logDebug := map[string]interface{}{
		common.InstanceID: i.InstanceID,
		stmtKey:           createStmt,
	}
	loggerI.Info("executing create instance statement", logDebug)
	if _, err = repo.neo4j.ExecStmt(repo.conn, createStmt, nil); err != nil {
		return errors.Wrap(err, "neo4j.ExecStmt returned an error")
	}

	loggerI.Info("create instance success", logDebug)
	return nil
}

// AddDimensions update dimensions list of the specified Instance node.
func (repo *InstanceRepository) AddDimensions(i *model.Instance) error {
	if i == nil {
		return errors.New("instance is required but was nil")
	}
	if len(i.InstanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}

	instanceLabel := fmt.Sprintf(instanceLabelFmt, i.GetID())
	stmt := fmt.Sprintf(addInstanceDimensionsStmt, instanceLabel)
	params := map[string]interface{}{dimensionsList: i.GetDimensions()}

	logDebug := map[string]interface{}{
		stmtKey:           stmt,
		stmtParamsKey:     params,
		common.InstanceID: i.InstanceID,
		dimensionsKey:     i.GetDimensions(),
	}
	loggerI.Info("executing add dimensions statement", logDebug)
	if _, err := repo.neo4j.ExecStmt(repo.conn, stmt, params); err != nil {
		return errors.Wrap(err, "neo4j.ExecStmt returned an error")
	}

	loggerI.Info("add instance dimensions success", logDebug)
	return nil
}

// Exists returns true if an instance already exists with the provided instanceID.
func (repo *InstanceRepository) Exists(i *model.Instance) (bool, error) {
	countStmt := fmt.Sprintf(countInstanceStmt, fmt.Sprintf(instanceLabelFmt, i.GetID()))

	loggerI.Info("executing instance exists query", log.Data{stmtKey: countStmt})
	rows, err := repo.neo4j.Query(repo.conn, countStmt, nil)
	if err != nil {
		return false, errors.Wrap(err, "neo4j.Query returned an error")
	}

	data := rows.Data[0]
	instanceCount, ok := data[0].(int64)
	if !ok {
		return false, errors.New("unexpected error while attempting to convert value to int64")
	}

	return instanceCount >= 1, nil
}

// Delete - delete an instance and all dimensions and relationships it has.
func (repo *InstanceRepository) Delete(i *model.Instance) error {
	logData := log.Data{common.InstanceID: i.InstanceID}
	instanceLabel := fmt.Sprintf(instanceLabelFmt, i.GetID())

	stmt := fmt.Sprintf(removeInstanceDimensionsAndRelationships, instanceLabel)
	logData[stmtKey] = stmt
	loggerI.Info("executing delete instance statement", logData)

	results, err := repo.neo4j.ExecStmt(repo.conn, stmt, nil)

	if err != nil {
		return errors.Wrap(err, "neo4j.ExecStmt returned an error")
	}

	stats := results.Metadata()["stats"]
	if stats != nil {
		logData["stats"] = stats.(map[string]interface{})
	}
	loggerI.Info("instance deleted successfully", logData)
	return nil
}
