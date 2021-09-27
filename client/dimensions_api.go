package client

import (
	"context"
	"errors"

	dataset "github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
)

//go:generate moq -out ../mocks/dataset.go -pkg mocks . IClient

const packageName = "client.DatasetAPI"

// ErrHostEmpty is returned when a Client is created with an empty Host
var ErrHostEmpty = errors.New("api host is required but was empty")

// ErrNoDatasetAPI is returned when a dataset API Client is not provided
var ErrNoDatasetAPI = errors.New("dataset api is required but is not provided")

// ErrNoDatastore is returned when a data store is not provided
var ErrNoDatastore = errors.New("data store is required but is not provided")

// ErrInstanceIDEmpty is returned when an InstanceID is not provided to a call that requires it
var ErrInstanceIDEmpty = errors.New("instance id is required but is empty")

// ErrDimensionNil is returned when a dimension is not provided to a call that requires it
var ErrDimensionNil = errors.New("dimension is required but is nil")

//ErrDimensionIDEmpty is returned when a dimension with an empty ID is provided to a call that requires it
var ErrDimensionIDEmpty = errors.New("dimension.id is required but is empty")

// IClient is an interface that represents the required functionality from dataset client from dp-api-clients-go
type IClient interface {
	PatchInstanceDimensions(ctx context.Context, serviceAuthToken, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (eTag string, err error)
	GetInstanceDimensionsInBatches(ctx context.Context, serviceAuthToken, instanceID string, batchSize, maxWorkers int) (dimensions dataset.Dimensions, eTag string, err error)
	GetInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID, ifMatch string) (m dataset.Instance, eTag string, err error)
	Checker(ctx context.Context, state *healthcheck.CheckState) error
}

// DatasetAPI provides methods for getting dimensions for a given instanceID and updating the node_id of a specific dimension.
type DatasetAPI struct {
	AuthToken      string
	DatasetAPIHost string
	Client         IClient
	MaxWorkers     int
	BatchSize      int
}

// NewDatasetAPIClient validates the parameters and creates a new dataset API client from dp-api-clients-go library.
func NewDatasetAPIClient(cfg *config.Config) (*DatasetAPI, error) {
	if len(cfg.DatasetAPIAddr) == 0 {
		return nil, ErrHostEmpty
	}
	return &DatasetAPI{
		AuthToken:      cfg.ServiceAuthToken,
		DatasetAPIHost: cfg.DatasetAPIAddr,
		Client:         dataset.NewAPIClient(cfg.DatasetAPIAddr),
		MaxWorkers:     cfg.DatasetAPIMaxWorkers,
		BatchSize:      cfg.DatasetAPIBatchSize,
	}, nil
}

// GetInstance retrieve the specified instance from the Dataset API.
func (api DatasetAPI) GetInstance(ctx context.Context, instanceID string) (*model.Instance, error) {
	if len(instanceID) == 0 {
		return &model.Instance{}, ErrInstanceIDEmpty
	}
	datasetInstance, _, err := api.Client.GetInstance(ctx, "", api.AuthToken, "", instanceID, headers.IfMatchAnyETag)
	if err != nil {
		return nil, err
	}
	return model.NewInstance(&datasetInstance), err
}

// GetDimensions retrieve the dimensions of the specified instance from the Dataset API
func (api DatasetAPI) GetDimensions(ctx context.Context, instanceID string, ifMatch string) ([]*model.Dimension, error) {
	if len(instanceID) == 0 {
		return nil, ErrInstanceIDEmpty
	}

	dimensions, _, err := api.Client.GetInstanceDimensionsInBatches(ctx, api.AuthToken, instanceID, api.BatchSize, api.MaxWorkers)
	if err != nil {
		return nil, err
	}

	ret := []*model.Dimension{}
	for _, dim := range dimensions.Items {
		ret = append(ret, model.NewDimension(&dim))
	}
	return ret, nil
}

// PatchDimensionOption makes an HTTP patch request to update the node_id and/or order for multiple dimension options
func (api DatasetAPI) PatchDimensionOption(ctx context.Context, instanceID string, updates []*dataset.OptionUpdate) (string, error) {
	return api.Client.PatchInstanceDimensions(ctx, api.AuthToken, instanceID, nil, updates, headers.IfMatchAnyETag)
}
