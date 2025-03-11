package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	host       = "http://localhost:8080"
	instanceID = "1234567890"
	authToken  = "pa55w0rd"
	ifMatch    = "*"
)

var errMock = errors.New("broken")

// Dimensions returned by dataset API mock
var datasetDimensionOne = dataset.Dimension{DimensionID: "666_SEX_MALE", NodeID: "1111", Option: "Male"}
var datasetDimensionTwo = dataset.Dimension{DimensionID: "666_SEX_FEMALE", NodeID: "1112", Option: "Female"}
var datasetDimensions = dataset.Dimensions{Items: []dataset.Dimension{datasetDimensionOne, datasetDimensionTwo}}

// Expected dimensions in dp-dimension-importer

var dimensionOne = model.NewDimension(&datasetDimensionOne)
var dimensionTwo = model.NewDimension(&datasetDimensionTwo)

var expectedDimensions = []*model.Dimension{dimensionOne, dimensionTwo}

// Instance returned by dataset API mock
var datasetInstance = dataset.Instance{Version: dataset.Version{ID: instanceID, CSVHeader: []string{"the", "csv", "header"}}}

// Instance in dp-dimension-importer
var expectedInstance = model.NewInstance(&datasetInstance)

var ctx = context.Background()

func TestNewClient(t *testing.T) {
	Convey("Given that NewDatasetAPIClient is called with an empty host", t, func() {
		datasetAPI, err := client.NewDatasetAPIClient(&config.Config{
			ServiceAuthToken: authToken,
			DatasetAPIAddr:   "",
		})

		Convey("Then a nil instance and ErrHostEmpty is returned", func() {
			So(datasetAPI, ShouldEqual, nil)
			So(err.Error(), ShouldEqual, "error creating new dataset api client: api host is required but was empty")
		})
	})
}

func TestGetInstance(t *testing.T) {
	Convey("Given valid client configuration", t, func() {
		clientMock := &mocks.IClientMock{
			GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
				return datasetInstance, "", nil
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When the GetInstance method is called", func() {
			instance, err := datasetAPI.GetInstance(ctx, instanceID)

			Convey("Then the expected response is returned with no error", func() {
				So(instance, ShouldResemble, expectedInstance)
				So(err, ShouldEqual, nil)
			})

			Convey("Then dataset.GetInstance is called exactly once with the right parameters", func() {
				So(len(clientMock.GetInstanceCalls()), ShouldEqual, 1)
				So(clientMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.GetInstanceCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
				So(clientMock.GetInstanceCalls()[0].UserAuthToken, ShouldEqual, "")
			})
		})
	})

	Convey("Given an empty instanceID", t, func() {
		instanceID := ""
		clientMock := &mocks.IClientMock{}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When GetInstance method is called", func() {
			instance, err := datasetAPI.GetInstance(ctx, instanceID)

			Convey("Then the expected error is returned", func() {
				So(instance, ShouldResemble, &model.Instance{})
				So(err.Error(), ShouldEqual, "error getting instance: instance id is required but is empty")
			})
		})
	})

	Convey("Given dataset.GetInstance will return an error", t, func() {
		clientMock := &mocks.IClientMock{
			GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
				return dataset.Instance{}, "", errMock
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When GetInstance is invoked", func() {
			instance, err := datasetAPI.GetInstance(ctx, instanceID)

			Convey("Then the expected error response is returned", func() {
				So(instance, ShouldEqual, nil)
				So(err, ShouldResemble, errMock)
			})

			Convey("Then dataset.GetInstance is called exactly once with the right parameters", func() {
				So(len(clientMock.GetInstanceCalls()), ShouldEqual, 1)
				So(clientMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.GetInstanceCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
				So(clientMock.GetInstanceCalls()[0].UserAuthToken, ShouldEqual, "")
			})
		})
	})
}

func TestGetDimensions(t *testing.T) {
	Convey("Given a valid client configuration", t, func() {
		clientMock := &mocks.IClientMock{
			GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, bacthSize, maxWorkers int) (dataset.Dimensions, string, error) {
				return datasetDimensions, "", nil
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When the client is called with a valid instanceID", func() {
			dims, err := datasetAPI.GetDimensions(ctx, instanceID, ifMatch)

			Convey("Then the expected response is returned with no error", func() {
				So(dims, ShouldResemble, expectedDimensions)
				So(err, ShouldEqual, nil)
			})

			Convey("Then dataset.GetInstanceDimensions is called exactly once with the right parameters", func() {
				So(len(clientMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
				So(clientMock.GetInstanceDimensionsInBatchesCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.GetInstanceDimensionsInBatchesCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
			})
		})
	})

	Convey("Given an empty instanceID is provided", t, func() {
		clientMock := &mocks.IClientMock{}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When GetDimensions is invoked", func() {
			dims, err := datasetAPI.GetDimensions(ctx, "", ifMatch)

			Convey("Then the expected error is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err.Error(), ShouldEqual, "error getting dimensions: instance id is required but is empty")
			})
		})
	})

	Convey("Given dataset.GetInstanceDimensions will return an error", t, func() {
		clientMock := &mocks.IClientMock{
			GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, bacthSize, maxWorkers int) (dataset.Dimensions, string, error) {
				return dataset.Dimensions{}, "", errMock
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When GetDimensions is invoked", func() {
			dims, err := datasetAPI.GetDimensions(ctx, instanceID, ifMatch)

			Convey("Then the expected error response is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldResemble, errMock)
			})

			Convey("Then dataset.GetInstanceDimensions is called exactly once with the right parameters", func() {
				So(len(clientMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
				So(clientMock.GetInstanceDimensionsInBatchesCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.GetInstanceDimensionsInBatchesCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
			})
		})
	})
}

func TestDatasetAPI_PatchDimensionOption(t *testing.T) {
	updates := []*dataset.OptionUpdate{
		{
			Name:   dimensionOne.DBModel().DimensionID,
			Option: dimensionOne.DBModel().Option,
			NodeID: dimensionOne.DBModel().NodeID,
		},
	}

	Convey("Given dataset.PatchInstanceDimensions will return an error", t, func() {
		clientMock := &mocks.IClientMock{
			PatchInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
				return "", errMock
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When PatchDimensionOption is called with an update", func() {
			_, err := datasetAPI.PatchDimensionOption(ctx, instanceID, updates)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errMock)
			})

			Convey("Then dataset.PatchInstanceDimensions is called exactly once with the right parameters", func() {
				So(len(clientMock.PatchInstanceDimensionsCalls()), ShouldEqual, 1)
				So(clientMock.PatchInstanceDimensionsCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
				So(clientMock.PatchInstanceDimensionsCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.PatchInstanceDimensionsCalls()[0].Upserts, ShouldBeNil)
				So(clientMock.PatchInstanceDimensionsCalls()[0].Updates, ShouldResemble, updates)
				So(clientMock.PatchInstanceDimensionsCalls()[0].IfMatch, ShouldEqual, headers.IfMatchAnyETag)
			})
		})
	})

	Convey("Given dataset.PatchInstanceDimensionOption succeeds", t, func() {
		clientMock := &mocks.IClientMock{
			PatchInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
				return "", nil
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When PatchDimensionOption is called", func() {
			_, err := datasetAPI.PatchDimensionOption(ctx, instanceID, updates)

			Convey("Then no error is returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("Then dataset.PatchInstanceDimensionOption is called exactly once with the right parameters", func() {
				So(len(clientMock.PatchInstanceDimensionsCalls()), ShouldEqual, 1)
				So(clientMock.PatchInstanceDimensionsCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
				So(clientMock.PatchInstanceDimensionsCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.PatchInstanceDimensionsCalls()[0].Upserts, ShouldBeNil)
				So(clientMock.PatchInstanceDimensionsCalls()[0].Updates, ShouldResemble, updates)
				So(clientMock.PatchInstanceDimensionsCalls()[0].IfMatch, ShouldEqual, headers.IfMatchAnyETag)
			})
		})
	})
}

func Response(body []byte, statusCode int, err error) (*http.Response, error) {
	reader := bytes.NewBuffer(body)
	readCloser := io.NopCloser(reader)

	return &http.Response{
		StatusCode: statusCode,
		Body:       readCloser,
	}, err
}
