package repository

import (
	"github.com/ONSdigital/go-ns/log"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"errors"
	"fmt"
)

const (
	// Create an Insatnce node.
	createInstanceStmt = "CREATE (i:%s) RETURN i"

	// Update the Instance node with the list of dimension types it contains.
	addInstanceDimensionsStmt = "MATCH (i:%s) SET i.dimensions = {dimensions_list}"

	instanceIDReqErr             = "Instance.InstanceID is required but was empty"
	createInstanceExecErr        = "Error while executing to create Instance statement."
	createInstanceSuccess        = "Successfully created Instance node."
	addInstanceDimensionsExecErr = "Error while executing add instance dimensions statement"
	addInstanceDimensionsSuccess = "Successfully added dimensions to Instance node"
)

type InstanceRepository struct {
	Neo NeoDatabase
}

func (repo *InstanceRepository) Create(i *model.Instance) error {
	if len(i.InstanceID) == 0 {
		err := errors.New(instanceIDReqErr)
		log.ErrorC(instanceIDReqErr, err, nil)
		return err
	}

	stmt := fmt.Sprintf(createInstanceStmt, i.GetLabel())
	logDebug := map[string]interface{}{
		logKeys.InstanceID: i.InstanceID,
		stmtKey:            stmt,
	}

	if _, err := repo.Neo.ExecStmt(stmt, nil); err != nil {
		log.ErrorC(createInstanceExecErr, err, logDebug)
		return err
	}

	log.Debug(createInstanceSuccess, logDebug)
	return nil
}

func (repo *InstanceRepository) AddDimensions(i *model.Instance) error {
	// validate i
	stmt := fmt.Sprintf(addInstanceDimensionsStmt, i.GetLabel())
	params := map[string]interface{}{dimensionsList: i.GetDimensions()}

	logDebug := map[string]interface{}{
		stmtKey:            stmt,
		stmtParamsKey:      params,
		logKeys.InstanceID: i.InstanceID,
		dimensionsKey:      i.GetDimensions(),
	}

	if _, err := repo.Neo.ExecStmt(stmt, params); err != nil {
		log.ErrorC(addInstanceDimensionsExecErr, err, nil)
		return err
	}

	log.Debug(addInstanceDimensionsSuccess, logDebug)
	return nil
}
