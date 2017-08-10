package repository

import (
	"errors"
	"fmt"

	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
)

//go:generate moq -out ../mocks/repository_generated_mocks.go -pkg mocks . Neo4jClient

const (
	// Create an Insatnce node.
	createInstanceStmt = "CREATE (i:`%s`) RETURN i"

	// Update the Instance node with the list of dimension types it contains.
	addInstanceDimensionsStmt = "MATCH (i:`%s`) SET i.dimensions = {dimensions_list}"

	instanceNilErr               = "Instance is require but was nil"
	instanceIDReqErr             = "Instance.InstanceID is required but was empty"
	createInstanceExecErr        = "Error while executing to create Instance statement."
	createInstanceSuccess        = "Successfully created Instance node."
	addInstanceDimensionsExecErr = "Error while executing add instance dimensions statement"
	addInstanceDimensionsSuccess = "Successfully added dimensions to Instance node"
)

// InstanceRepository provides functionality for insterting & updating Instances into a Neo4j graph database
type InstanceRepository struct {
	Neo4j Neo4jClient
}

// Create creates an Instance node in a Neo4j graph database
func (repo *InstanceRepository) Create(i *model.Instance) error {
	if i == nil {
		err := errors.New(instanceNilErr)
		log.Error(err, nil)
		return err
	}
	if len(i.InstanceID) == 0 {
		err := errors.New(instanceIDReqErr)
		log.ErrorC(instanceIDReqErr, err, nil)
		return err
	}

	instanceLabel := fmt.Sprintf(instanceLabelFmt, i.GetID())
	stmt := fmt.Sprintf(createInstanceStmt, instanceLabel)

	logDebug := map[string]interface{}{
		logKeys.InstanceID: i.InstanceID,
		stmtKey:            stmt,
	}

	if _, err := repo.Neo4j.ExecStmt(stmt, nil); err != nil {
		log.ErrorC(createInstanceExecErr, err, logDebug)
		return err
	}

	log.Debug(createInstanceSuccess, logDebug)
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
		stmtKey:            stmt,
		stmtParamsKey:      params,
		logKeys.InstanceID: i.InstanceID,
		dimensionsKey:      i.GetDimensions(),
	}

	if _, err := repo.Neo4j.ExecStmt(stmt, params); err != nil {
		log.ErrorC(addInstanceDimensionsExecErr, err, nil)
		return err
	}

	log.Debug(addInstanceDimensionsSuccess, logDebug)
	return nil
}
