package store

import (
	"context"

	"github.com/ONSdigital/dp-graph/models"
)

//go:generate moq -out storertest/storer.go -pkg storertest . Storer

// Storer is an interface representing the required methods to interact with the DB for instances and dimensions
type Storer interface {
	CreateInstanceConstraint(ctx context.Context, instance *models.Instance) error
	CreateInstance(ctx context.Context, instance *models.Instance) error
	AddDimensions(ctx context.Context, instance *models.Instance) error
	CreateCodeRelationship(ctx context.Context, instance *models.Instance, codeListID, code string) error
	InstanceExists(ctx context.Context, instance *models.Instance) (bool, error)
	CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error)
	AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error
	SetInstanceIsPublished(ctx context.Context, instanceID string) error
	InsertDimension(ctx context.Context, cache map[string]string, instance *models.Instance, dimension *models.Dimension) (*models.Dimension, error)
	Close(ctx context.Context) error
}
