package client

import (
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"os"
	"sync"
	"github.com/ONSdigital/dp-dimension-importer/common"
)

const dimensionNodeIDFMT = "_%s_%s_%s"
const insertDimensionStmt = "CREATE (d:%s {name: {name}, value: {value}})"
const nameKey = "name"
const valueKey = "value"

var errDimensionNameRequired = errors.New("dimension.NodeName is required but was empty")
var errDimensionValueRequired = errors.New("dimension.Value is required but was empty")
var errDimensionEmpty = errors.New("dimenion invalid: both dimension.NodeName and dimension.Value are required but were both empty")

const openConnErrMsg = "Unexpected error when attempting to open bolt connection"

var once sync.Once

var neo *Neo4jClient

// Neo4jClient client providing functions for inserting DimensionsKey into a Neo4j Graph DB.
type Neo4jClient struct {
	URL        string
	driverPool bolt.DriverPool
}

// NeoClientInstance creates and return a singleton instance of Neo4jClient if it has not already been created,
// otherwise return the existing instance.
func NeoClientInstance(url string, poolSize int) *Neo4jClient {
	once.Do(func() {
		driverPool, err := bolt.NewDriverPool(url, poolSize)
		if err != nil {
			log.ErrorC("Could not create bolt driver pool", err, log.Data{
				"url":      url,
				"poolSize": poolSize,
			})
			os.Exit(1)
		}
		neo = &Neo4jClient{URL: url, driverPool: driverPool}
	})
	return neo
}

func (neo *Neo4jClient) BatchInsert(instanceID string, dimensions []model.Dimension) error {
	logData := log.Data{
		common.InstanceIDKey: instanceID,
	}

	if err := neo.validate(instanceID, dimensions); err != nil {
		return err
	}

	conn, err := neo.driverPool.OpenPool()
	if err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return err
	}

	defer conn.Close()

	batch := NewBatchJob(instanceID, dimensions)
	pipelineStmt, err := conn.PreparePipeline(batch.stmts...)

	if err != nil {
		log.ErrorC("Error creating pipelineStatement", err, logData)
		return err
	}

	defer func() {
		if err = pipelineStmt.Close(); err != nil {
			log.ErrorC("Error while attempting to close statement", err, logData)
			os.Exit(1)
		}
	}()

	_, err = pipelineStmt.ExecPipeline(batch.params...)
	if err != nil {
		log.ErrorC("Error executing pipelineStatements", err, logData)
		return err
	}

	log.Debug("Batch insert successful", batch.debug)
	return nil
}

func (c *Neo4jClient) validate(instanceID string, dimensions []model.Dimension) error {
	if len(instanceID) == 0 {
		return common.ErrInstanceIDRequired
	}
	for _, d := range dimensions {
		if len(d.NodeName) == 0 && len(d.Value) == 0 {
			return errDimensionEmpty
		}
		if len(d.NodeName) == 0 {
			return errDimensionNameRequired
		}
		if len(d.Value) == 0 {
			return errDimensionValueRequired
		}
	}
	return nil
}

type batchJob struct {
	instanceID string
	params     []map[string]interface{}
	stmts      []string
	batchSize  int
	debug      map[string]interface{}
}

func NewBatchJob(instanceID string, dimensions []model.Dimension) *batchJob {
	b := &batchJob{
		instanceID: instanceID,
		params:     make([]map[string]interface{}, 0),
		stmts:      make([]string, 0),
		debug:      make(map[string]interface{}),
	}

	for _, d := range dimensions {
		dimensionNodeID := fmt.Sprintf(dimensionNodeIDFMT, b.instanceID, d.NodeName, d.Value)
		createStmt := fmt.Sprintf(insertDimensionStmt, dimensionNodeID)
		params := map[string]interface{}{
			nameKey:  d.NodeName,
			valueKey: d.Value,
		}

		b.stmts = append(b.stmts, createStmt)
		b.params = append(b.params, params)

		b.debug[dimensionNodeID] = []interface{}{params}
	}
	return b
}
