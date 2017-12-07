package repository

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
)

const (
	uniqueObsConstraintStmt = "CREATE CONSTRAINT ON (o:`%s`) ASSERT o.rowIndex IS UNIQUE"
	observationLabelFmt     = "_%s_observation"
)

// ObservationRepository provides observation related storage functionality.
type ObservationRepository struct {
	neo4j Neo4jClient
	conn  bolt.Conn
	log   logging.Logger
}

// NewObservationRepository returns a new ObservationRepository. A bolt.Conn will be obtained from the supplied connectionPool.
// The obtained bolt.Conn will be used for the life time of the ObservationRepository struct
// - it is the responsibility of the caller to call Close when they have finished.
func NewObservationRepository(connPool common.NeoDriverPool, neo Neo4jClient) (*ObservationRepository, error) {
	conn, err := connPool.OpenPool()
	if err != nil {
		return nil, errors.Wrap(err, "connPool.OpenPool returned an error")
	}

	logger := logging.Logger{Name: "repository.ObservationRepository"}

	return &ObservationRepository{
		neo4j: neo,
		conn:  conn,
		log:   logger,
	}, nil
}

// Close - closes an open resources held by the ObservationRepository.
func (repo ObservationRepository) Close() {

	if repo.conn != nil {
		if err := repo.conn.Close(); err != nil {
			repo.log.ErrorC("conn.Close returned an error", err, nil)
		}
	}

	repo.log.Info("conn closed successfully", nil)
}

// CreateConstraint creates a constraint on observations inserted for this instance.
func (repo ObservationRepository) CreateConstraint(i *model.Instance) error {
	var err error

	if i == nil {
		return errors.New("instance is required but was nil")
	}
	if len(i.InstanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}

	instanceLabel := fmt.Sprintf(observationLabelFmt, i.GetID())
	createStmt := fmt.Sprintf(uniqueObsConstraintStmt, instanceLabel)

	logDebug := map[string]interface{}{
		"instance_id": i.InstanceID,
		"statement":   createStmt,
	}
	repo.log.Info("executing create instance statement", logDebug)

	if _, err = repo.neo4j.ExecStmt(repo.conn, createStmt, nil); err != nil {
		return errors.Wrap(err, "neo4j.ExecStmt returned an error when creating observation constraint")
	}

	repo.log.Info("created observation constraint", logDebug)
	return nil
}
