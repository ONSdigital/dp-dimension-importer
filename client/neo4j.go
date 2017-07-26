package client

import (
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"os"
	"sync"
	"strconv"
)

const dimensionNodeIDFMT = "_%s"
const insertDimensionStmt = "CREATE (d: _%s {value: {value}}) RETURN ID(d)"
const dimensionIDKey = "dimension_id"
const valueKey = "value"

var errDimensionIDRequired = errors.New("dimension.Dimension_ID is required but was empty")
var errDimensionValueRequired = errors.New("dimension.Value is required but was empty")
var errDimensionEmpty = errors.New("dimenion invalid: both dimension.dimension_id and dimension.value are required but were both empty")

const openConnErrMsg = "Unexpected error when attempting to open bolt connection"

var once sync.Once

var driverPool bolt.DriverPool

type Neo4j struct {}

func InitDB(url string, poolSize int) Neo4j {
	once.Do(func() {
		pool, err := bolt.NewDriverPool(url, poolSize)
		if err != nil {
			log.ErrorC("Could not create bolt driver pool", err, log.Data{
				"url":      url,
				"poolSize": poolSize,
			})
			os.Exit(1)
		}
		driverPool = pool
	})
	return Neo4j{}
}

func (db Neo4j) InsertDimension(instanceID string, d *model.Dimension) (*model.Dimension, error) {
	logData := log.Data{
		common.InstanceIDKey: instanceID,
	}

	if err := validate(instanceID, d); err != nil {
		return nil, err
	}

	conn, err := driverPool.OpenPool()
	if err != nil {
		log.ErrorC(openConnErrMsg, err, nil)
		return nil, err
	}

	defer conn.Close()

	createStmt := fmt.Sprintf(insertDimensionStmt, d.Dimension_ID)
	stmt, err := conn.PrepareNeo(createStmt)

	if err != nil {
		log.ErrorC("Error creating statement.", err, logData)
		return nil, err
	}
	defer stmt.Close()

	params := map[string]interface{}{valueKey: d.Value}
	rows, err := stmt.QueryNeo(params)

	if err != nil {
		log.ErrorC("Error executing statement", err, logData)
		return nil, err
	}

	data, _, _ := rows.NextNeo()

	nodeID, _ := data[0].(int64)
	d.NodeId = strconv.FormatInt(nodeID, 10)
	return d, nil
}

func validate(instanceID string, d *model.Dimension) error {
	if len(instanceID) == 0 {
		return common.ErrInstanceIDRequired
	}
	if d == nil {
		return errors.New("dimension is nil")
	}
	if len(d.Dimension_ID) == 0 && len(d.Value) == 0 {
		return errDimensionEmpty
	}
	if len(d.Dimension_ID) == 0 {
		return errDimensionIDRequired
	}
	if len(d.Value) == 0 {
		return errDimensionValueRequired
	}
	return nil
}
