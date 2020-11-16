package store

import (
	"context"

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
)

//go:generate moq -out storertest/storer.go -pkg storertest . Storer

// Storer is an interface representing the required methods to interact with the DB for instances and dimensions
type Storer interface {
	CreateInstanceConstraint(ctx context.Context, instanceID string) error
	CreateInstance(ctx context.Context, instanceID string, csvHeaders []string) error
	AddDimensions(ctx context.Context, instanceID string, dimensions []interface{}) error
	CreateCodeRelationship(ctx context.Context, instanceID, codeListID, code string) error
	InstanceExists(ctx context.Context, instanceID string) (bool, error)
	InsertDimension(ctx context.Context, cache map[string]string, instanceID string, dimension *models.Dimension) (*models.Dimension, error)
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	Close(ctx context.Context) error
	ErrorChan() chan error
}
