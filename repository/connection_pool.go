package repository

import (
	"sync"

	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const errDrivePoolInit = "error while attempting to initialize neo4j driver pool"

var once sync.Once
var pool common.NeoDriverPool

// NewConnectionPool creates a new connection pool for a Neo4j database.
func NewConnectionPool(url string, poolSize int) (common.NeoDriverPool, error) {
	var err error

	once.Do(func() {
		pool, err = bolt.NewDriverPool(url, poolSize)
	})

	if err != nil {
		log.ErrorC(errDrivePoolInit, err, log.Data{
			"url":       url,
			"pool_size": poolSize,
		})
	}
	return pool, nil
}
