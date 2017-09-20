package repository

import (
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/go-ns/log"
	"sync"
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
			common.URL:      url,
			common.PoolSize: poolSize,
		})
	}
	return pool, nil
}
