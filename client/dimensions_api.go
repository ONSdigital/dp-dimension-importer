package client

import (
	"context"
	"errors"

	"net/url"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
)

//go:generate moq -out ../mocks/dataset.go -pkg mocks . IClient

const packageName = "client.DatasetAPI"

// ErrHostEmpty is returned when a Client is created with an empty Host
var ErrHostEmpty = errors.New("validation error: api host is required but was empty")

// ErrInstanceIDEmpty is returned when an InstanceID is not provided to a call that requires it
var ErrInstanceIDEmpty = errors.New("validation error: instance id is required but is empty")

// ErrDimensionNil is returned when a dimension is not provided to a call that requires it
var ErrDimensionNil = errors.New("dimension is required but is nil")

//ErrDimensionIDEmpty is returned when a dimension with an empty ID is provided to a call that requires it
var ErrDimensionIDEmpty = errors.New("dimension.id is required but is empty")

// IClient is an interface that represents the required functionality from dataset client from dp-api-clients-go
type IClient interface {
	PatchInstanceDimensionOption(ctx context.Context, serviceAuthToken, instanceID, dimensionID, optionID, nodeID string, order *int) error
	GetInstanceDimensionsInBatches(ctx context.Context, serviceAuthToken, instanceID string, batchSize, maxWorkers int) (m dataset.Dimensions, err error)
	GetInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID string) (m dataset.Instance, err error)
	Checker(ctx context.Context, state *healthcheck.CheckState) error
}

// DatasetAPI provides methods for getting dimensions for a given instanceID and updating the node_id of a specific dimension.
type DatasetAPI struct {
	AuthToken         string
	DatasetAPIHost    string
	Client            IClient
	EnablePatchNodeID bool
	MaxWorkers        int
	BatchSize         int
}

// NewDatasetAPIClient validates the parameters and creates a new dataset API client from dp-api-clients-go library.
func NewDatasetAPIClient(cfg *config.Config) (*DatasetAPI, error) {
	if len(cfg.DatasetAPIAddr) == 0 {
		return nil, ErrHostEmpty
	}
	return &DatasetAPI{
		AuthToken:         cfg.ServiceAuthToken,
		DatasetAPIHost:    cfg.DatasetAPIAddr,
		Client:            dataset.NewAPIClient(cfg.DatasetAPIAddr),
		EnablePatchNodeID: cfg.EnablePatchNodeID,
		MaxWorkers:        cfg.DatasetAPIMaxWorkers,
		BatchSize:         cfg.DatasetAPIBatchSize,
	}, nil
}

// GetInstance retrieve the specified instance from the Dataset API.
func (api DatasetAPI) GetInstance(ctx context.Context, instanceID string) (*model.Instance, error) {
	if len(instanceID) == 0 {
		return &model.Instance{}, ErrInstanceIDEmpty
	}
	datasetInstance, err := api.Client.GetInstance(ctx, "", api.AuthToken, "", instanceID)
	if err != nil {
		return nil, err
	}
	return model.NewInstance(&datasetInstance), err
}

// GetDimensions retrieve the dimensions of the specified instance from the Dataset API
func (api DatasetAPI) GetDimensions(ctx context.Context, instanceID string) ([]*model.Dimension, error) {
	if len(instanceID) == 0 {
		return nil, ErrInstanceIDEmpty
	}

	dimensions, err := api.Client.GetInstanceDimensionsInBatches(ctx, api.AuthToken, instanceID, api.BatchSize, api.MaxWorkers)
	if err != nil {
		return nil, err
	}

	ret := []*model.Dimension{}
	for _, dim := range dimensions.Items {
		ret = append(ret, model.NewDimension(&dim))
	}
	return ret, nil
}

// PatchDimensionOption make a HTTP patch request to update the node_id and/or order of the specified dimension.
func (api DatasetAPI) PatchDimensionOption(ctx context.Context, instanceID string, d *model.Dimension, order *int) error {
	if len(instanceID) == 0 {
		return ErrInstanceIDEmpty
	}
	if d == nil {
		return ErrDimensionNil
	}
	dim := d.DbModel()
	if dim == nil {
		return ErrDimensionNil
	}
	if len(dim.DimensionID) == 0 {
		return ErrDimensionIDEmpty
	}

	nodeID := ""
	if api.EnablePatchNodeID {
		nodeID = dim.NodeID
	}

	return api.Client.PatchInstanceDimensionOption(ctx, api.AuthToken, instanceID, dim.DimensionID, url.PathEscape(dim.Option), nodeID, order)
}
