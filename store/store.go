package store

import (
	"context"

	"github.com/ONSdigital/dp-dimension-importer/model"
)

//go:generate moq -out storertest/storer.go -pkg storertest . Storer

type Storer interface {
	CreateInstanceConstraint(ctx context.Context, instance *model.Instance) error
	CreateInstance(ctx context.Context, instance *model.Instance) error
	AddDimensions(ctx context.Context, instance *model.Instance) error
	CreateCodeRelationship(ctx context.Context, instance *model.Instance, codeListID, code string) error
	InstanceExists(ctx context.Context, instance *model.Instance) (bool, error)
	CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error)
	AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error
	SetInstanceIsPublished(ctx context.Context, instanceID string) error
	InsertDimension(ctx context.Context, cache map[string]string, instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error)
	Close(ctx context.Context) error
}
