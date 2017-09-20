package repository

import (
	"errors"
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/ONSdigital/go-ns/log"
	"strings"
)

const (
	// Create an Insatnce node.
	createInstanceStmt = "CREATE (i:`%s` { header:'%s'}) RETURN i"

	// Count the instances with this ID.
	countInstanceStmt = "MATCH (i: `%s`) RETURN COUNT(*)"

	// Delete an intsance node & all of the dimensions relating to it.
	removeInstanceDimensionsAndRelationships = "MATCH (n)<-[:HAS_DIMENSION]-(i:`%s`) DETACH DELETE n, i"

	// Update the Instance node with the list of dimension types it contains.
	addInstanceDimensionsStmt = "MATCH (i:`%s`) SET i.dimensions = {dimensions_list}"

	instanceNilErr               = "instance is required but was nil"
	instanceIDReqErr             = "instance id is required but was empty"
	createInstanceExecErr        = "error while executing to create Instance statement"
	createInstanceSuccess        = "successfully created instance node"
	addInstanceDimensionsExecErr = "error while executing add instance dimensions statement"
	addInstanceDimensionsSuccess = "successfully added dimensions to instance node"
	instanceCountQueryErr        = "Unexpected error while attempting to count instance nodes"
	castToint64Err               = "unexpected error while attempting to convert value to int64"
	deleteInstanceErr            = "unexpected error while attempting to delete instance node"
	removeInstanceSuccess        = "successfully deleted instance and all of its dimensions and relationships"
	removeStatsKey               = "stats"
	removeInstanceErr            = "unexpected error while attempting to remove instance and its dimensions"
	closeErr                     = "unexpected error while attempting to close neo4j conn"
	openConnErr                  = "unexpected error while attempting to open neo4j conn"
)

// NewInstanceRepository creates a new InstanceRepository. A bolt.Conn will be obtained from the supplied connectionPool.
// The obtained bolt.Conn will be used for the life time of the InstanceRepository struct
// - it is the responsibility of the caller to call Close when they have finished.
func NewInstanceRepository(connPool common.NeoDriverPool, neo Neo4jClient) (*InstanceRepository, error) {
	conn, err := connPool.OpenPool()
	if err != nil {
		log.ErrorC(openConnErr, err, nil)
		return nil, err
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
			log.ErrorC(closeErr, err, nil)
		}
	}
	log.Info("InstanceRepository conn closed", nil)
}

// Create creates an Instance node in a neo4j graph database
func (repo *InstanceRepository) Create(i *model.Instance) error {
	var err error

	if i == nil {
		err = errors.New(instanceNilErr)
		log.Error(err, nil)
		return err
	}
	if len(i.InstanceID) == 0 {
		err = errors.New(instanceIDReqErr)
		log.ErrorC(instanceIDReqErr, err, nil)
		return err
	}

	instanceLabel := fmt.Sprintf(instanceLabelFmt, i.GetID())
	createStmt := fmt.Sprintf(createInstanceStmt, instanceLabel, strings.Join(i.CSVHeader, ","))

	logDebug := map[string]interface{}{
		common.InstanceID: i.InstanceID,
		stmtKey:           createStmt,
	}

	if _, err = repo.neo4j.ExecStmt(repo.conn, createStmt, nil); err != nil {
		log.ErrorC(createInstanceExecErr, err, logDebug)
		return err
	}

	log.Info(createInstanceSuccess, logDebug)
	return nil
}

// AddDimensions update dimensions list of the specified Instance node.
func (repo *InstanceRepository) AddDimensions(i *model.Instance) error {
	if i == nil {
		return errors.New(instanceNilErr)
	}
	if len(i.InstanceID) == 0 {
		return errors.New(instanceIDReqErr)
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

	if _, err := repo.neo4j.ExecStmt(repo.conn, stmt, params); err != nil {
		log.ErrorC(addInstanceDimensionsExecErr, err, nil)
		return err
	}

	log.Info(addInstanceDimensionsSuccess, logDebug)
	return nil
}

// Exists returns true if an instance already exists with the provided instanceID.
func (repo *InstanceRepository) Exists(i *model.Instance) (bool, error) {
	countStmt := fmt.Sprintf(countInstanceStmt, fmt.Sprintf(instanceLabelFmt, i.GetID()))
	rows, err := repo.neo4j.Query(repo.conn, countStmt, nil)
	if err != nil {
		log.ErrorC(instanceCountQueryErr, err, log.Data{common.InstanceID: i.GetID()})
		return false, err
	}

	data := rows.Data[0]
	instanceCount, ok := data[0].(int64)
	if !ok {
		return false, errors.New(castToint64Err)
	}

	return instanceCount >= 1, nil
}

// Delete - delete an instance and all dimensions and relationships it has.
func (repo *InstanceRepository) Delete(i *model.Instance) error {
	logData := log.Data{common.InstanceID: i.InstanceID}
	instanceLabel := fmt.Sprintf(instanceLabelFmt, i.GetID())

	stmt := fmt.Sprintf(removeInstanceDimensionsAndRelationships, instanceLabel)
	results, err := repo.neo4j.ExecStmt(repo.conn, stmt, nil)

	if err != nil {
		log.ErrorC(deleteInstanceErr, err, logData)
		return err
	}

	stats := results.Metadata()[removeStatsKey]
	if stats != nil {
		logData[removeStatsKey] = stats.(map[string]interface{})
	}
	log.Info(removeInstanceSuccess, logData)
	return nil
}
