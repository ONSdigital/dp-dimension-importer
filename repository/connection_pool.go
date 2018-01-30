package repository

import (
	"sync"

	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
)

const errDrivePoolInit = "error while attempting to initialize neo4j driver pool"

var once sync.Once
var pool bolt.ClosableDriverPool

// NewConnectionPool creates a new connection pool for a Neo4j database.
func NewConnectionPool(url string, poolSize int) (bolt.ClosableDriverPool, error) {
	var err error

	once.Do(func() {
		pool, err = bolt.NewClosableDriverPool(url, poolSize)
	})

	if err != nil {
		log.ErrorC(errDrivePoolInit, err, log.Data{
			"url":       url,
			"pool_size": poolSize,
		})
	}
	return pool, nil
}
