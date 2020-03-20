package client_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	host       = "http://localhost:8080"
	instanceID = "1234567890"
	authToken  = "pa55w0rd"
)

var errMock = errors.New("broken")

// Dimensions returned by dataset API mock
var datasetDimensionOne = dataset.Dimension{DimensionID: "666_SEX_MALE", NodeID: "1111", Option: "Male"}
var datasetDimensionTwo = dataset.Dimension{DimensionID: "666_SEX_FEMALE", NodeID: "1112", Option: "Female"}
var datasetDimensions = dataset.Dimensions{[]dataset.Dimension{datasetDimensionOne, datasetDimensionTwo}}

// Expected dimensions in dp-dimension-importer

var dimensionOne = model.NewDimension(&datasetDimensionOne)
var dimensionTwo = model.NewDimension(&datasetDimensionTwo)

var expectedDimensions = []*model.Dimension{dimensionOne, dimensionTwo}

// Instance returned by dataset API mock
var datasetInstance = dataset.Instance{dataset.Version{ID: instanceID, CSVHeader: []string{"the", "csv", "header"}}}

// Instance in dp-dimension-importer
var expectedInstance = model.NewInstance(&datasetInstance)

var ctx = context.Background()

func TestNewClient(t *testing.T) {

	Convey("Given that NewDatasetAPIClient is called with an empty host", t, func() {
		datasetAPI, err := client.NewDatasetAPIClient(authToken, "")

		Convey("Then a nil instance and ErrHostEmpty is returned", func() {
			So(datasetAPI, ShouldEqual, nil)
			So(err.Error(), ShouldResemble, client.ErrHostEmpty.Error())
		})
	})
}

func TestGetInstance(t *testing.T) {

	Convey("Given valid client configuration", t, func() {
		clientMock := &mocks.IClientMock{
			GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string) (dataset.Instance, error) {
				return datasetInstance, nil
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

			Convey("And dataset.GetInstance is called exactly once with the right parameters", func() {
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
				So(err, ShouldResemble, client.ErrInstanceIDEmpty)
			})
		})
	})

	Convey("Given dataset.GetInstance will return an error", t, func() {

		clientMock := &mocks.IClientMock{
			GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string) (dataset.Instance, error) {
				return dataset.Instance{}, errMock
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

			Convey("And dataset.GetInstance is called exactly once with the right parameters", func() {
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
			GetInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string) (dataset.Dimensions, error) {
				return datasetDimensions, nil
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When the client is called with a valid instanceID", func() {

			dims, err := datasetAPI.GetDimensions(ctx, instanceID)

			Convey("Then the expected response is returned with no error", func() {
				So(dims, ShouldResemble, expectedDimensions)
				So(err, ShouldEqual, nil)
			})

			Convey("And dataset.GetInstanceDimensions is called exactly once with the right parameters", func() {
				So(len(clientMock.GetInstanceDimensionsCalls()), ShouldEqual, 1)
				So(clientMock.GetInstanceDimensionsCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.GetInstanceDimensionsCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
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

			dims, err := datasetAPI.GetDimensions(ctx, "")

			Convey("Then the expected error is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldResemble, client.ErrInstanceIDEmpty)
			})
		})
	})

	Convey("Given dataset.GetInstanceDimensions will return an error", t, func() {

		clientMock := &mocks.IClientMock{
			GetInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string) (dataset.Dimensions, error) {
				return dataset.Dimensions{}, errMock
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When GetDimensions is invoked", func() {

			dims, err := datasetAPI.GetDimensions(ctx, instanceID)

			Convey("Then the expected error response is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldResemble, errMock)
			})

			Convey("And dataset.GetInstanceDimensions is called exactly once with the right parameters", func() {
				So(len(clientMock.GetInstanceDimensionsCalls()), ShouldEqual, 1)
				So(clientMock.GetInstanceDimensionsCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.GetInstanceDimensionsCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
			})
		})
	})
}

func TestDatasetAPI_PutDimensionNodeID(t *testing.T) {

	Convey("Given no instanceID is provided", t, func() {
		clientMock := &mocks.IClientMock{}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(ctx, "", dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, client.ErrInstanceIDEmpty)
			})
		})
	})

	Convey("Given no dimension is provided", t, func() {
		clientMock := &mocks.IClientMock{}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(ctx, instanceID, nil)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, client.ErrDimensionNil)
			})
		})
	})

	Convey("Given a dimension with an empty dimensionID", t, func() {
		clientMock := &mocks.IClientMock{}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			emptyDimension := model.NewDimension(&dataset.Dimension{})
			err := datasetAPI.PutDimensionNodeID(ctx, instanceID, emptyDimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, client.ErrDimensionIDEmpty)
			})
		})
	})

	Convey("Given dataset.PutInstanceDimensionOptionNodeID will return an error", t, func() {
		clientMock := &mocks.IClientMock{
			PutInstanceDimensionOptionNodeIDFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, dimensionID string, optionID string, nodeID string) error {
				return errMock
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(ctx, instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errMock)
			})

			Convey("And dataset.PutInstanceDimensionOptionNodeID is called exactly once with the right parameters", func() {
				So(len(clientMock.PutInstanceDimensionOptionNodeIDCalls()), ShouldEqual, 1)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].DimensionID, ShouldEqual, dimensionOne.DbModel().DimensionID)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].NodeID, ShouldEqual, dimensionOne.DbModel().NodeID)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].OptionID, ShouldEqual, dimensionOne.DbModel().Option)
			})
		})
	})

	Convey("Given dataset.PutInstanceDimensionOptionNodeID succeeds", t, func() {
		clientMock := &mocks.IClientMock{
			PutInstanceDimensionOptionNodeIDFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, dimensionID string, optionID string, nodeID string) error {
				return nil
			},
		}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			Client:         clientMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(ctx, instanceID, dimensionOne)

			Convey("Then no error is returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("And dataset.PutInstanceDimensionOptionNodeID is called exactly once with the right parameters", func() {
				So(len(clientMock.PutInstanceDimensionOptionNodeIDCalls()), ShouldEqual, 1)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].ServiceAuthToken, ShouldEqual, authToken)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].DimensionID, ShouldEqual, dimensionOne.DbModel().DimensionID)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].InstanceID, ShouldEqual, instanceID)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].NodeID, ShouldEqual, dimensionOne.DbModel().NodeID)
				So(clientMock.PutInstanceDimensionOptionNodeIDCalls()[0].OptionID, ShouldEqual, dimensionOne.DbModel().Option)
			})
		})
	})
}

func Response(body []byte, statusCode int, err error) (*http.Response, error) {
	reader := bytes.NewBuffer(body)
	readCloser := ioutil.NopCloser(reader)

	return &http.Response{
		StatusCode: statusCode,
		Body:       readCloser,
	}, err
}
