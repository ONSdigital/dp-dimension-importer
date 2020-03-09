package client_test

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"fmt"

	"strings"

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
var dimensionOne = &model.Dimension{DimensionID: "666_SEX_MALE", NodeID: "1111", Option: "Male"}
var dimensionTwo = &model.Dimension{DimensionID: "666_SEX_FEMALE", NodeID: "1112", Option: "Female"}

var expectedDimensions = model.DimensionNodeResults{
	Items: []*model.Dimension{dimensionOne, dimensionTwo},
}

var expectedInstance = &model.Instance{InstanceID: instanceID, CSVHeader: []string{"the", "csv", "header"}}

var body []byte

func TestDatasetAPI_GetInstance_NotConfigured(t *testing.T) {

	Convey("Given the client has not been configured", t, func() {
		httpClientMock := &mocks.HTTPClientMock{}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		// Set the host to an empty for this test case.
		datasetAPI := client.DatasetAPI{
			DatasetAPIHost:     "",
			HTTPClient:         httpClientMock,
			ResponseBodyReader: respBodyReaderMock,
		}

		Convey("When the GetInstance is called", func() {
			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then a nil instance and the appropriate error is returned.", func() {
				So(instance, ShouldEqual, nil)
				So(err.Error(), ShouldResemble, client.ErrHostEmpty.Error())
			})

			Convey("And HTTPClient.Do and ResponseBodyReader.ReadAll are never called", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 0)
				So(len(respBodyReaderMock.ReadCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestDatasetAPI_GetInstance(t *testing.T) {

	Convey("Given valid client configuration", t, func() {
		httpClientMock := &mocks.HTTPClientMock{}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		body, _ = json.Marshal(expectedInstance)
		reader := bytes.NewBuffer(body)
		readCloser := ioutil.NopCloser(reader)

		// set up mocks.
		httpClientMock.DoFunc = func(req *http.Request) (*http.Response, error) {
			return &http.Response{Body: readCloser, StatusCode: 200}, nil
		}

		respBodyReaderMock.ReadFunc = func(r io.Reader) ([]byte, error) {
			return body, nil
		}

		datasetAPI := client.DatasetAPI{
			DatasetAPIHost:     "http://localhost:8080",
			HTTPClient:         httpClientMock,
			ResponseBodyReader: respBodyReaderMock,
		}

		Convey("When the GetInstance method is called", func() {

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected response is returned with no error", func() {
				So(instance, ShouldResemble, expectedInstance)
				So(err, ShouldEqual, nil)
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is called 1 time with the expected parameters", func() {
				calls := respBodyReaderMock.ReadCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].R, ShouldResemble, readCloser)
			})
		})
	})
}

func TestDatasetAPI_GetInstance_EmptyInstanceID(t *testing.T) {

	Convey("Given an empty instanceID", t, func() {

		instanceID := ""

		httpClientMock := &mocks.HTTPClientMock{}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		datasetAPI := client.DatasetAPI{
			DatasetAPIHost:     "http://localhost:8080",
			HTTPClient:         httpClientMock,
			ResponseBodyReader: respBodyReaderMock,
		}

		Convey("When GetInstance is invoked", func() {

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error is returned", func() {
				So(instance, ShouldEqual, nil)
				So(err, ShouldResemble, client.ErrInstanceIDEmpty)
			})
		})
	})
}

func TestDatasetAPI_GetInstance_HTTPClientErr(t *testing.T) {

	Convey("Given HTTPClient.Do will return an error", t, func() {

		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, errMock
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		Convey("When GetInstance is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(instance, ShouldEqual, nil)

				url := fmt.Sprintf(client.GetInstanceURIFMT, datasetAPI.DatasetAPIHost, instanceID)
				expectedError := errors.Wrap(errMock, fmt.Sprintf("HTTPClient.Do returned an error when attempting to make request: method: %s, url: %s", "GET", url))
				So(err.Error(), ShouldResemble, expectedError.Error())
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is never called", func() {
				So(len(respBodyReaderMock.ReadCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestDatasetAPI_GetInstance_HTTPErrResponse(t *testing.T) {
	Convey("Given HTTPClient.Do returns a non 200 response status", t, func() {
		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return Response([]byte{}, 400, nil)
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		Convey("When GetInstance is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(instance, ShouldEqual, nil)

				url := fmt.Sprintf(client.GetInstanceURIFMT, datasetAPI.DatasetAPIHost, instanceID)
				expectedErr := errors.Errorf("incorrect status code: expected: %d, actual: %d, url: %s, method: %s", 200, 400, url, "GET")
				So(err.Error(), ShouldResemble, expectedErr.Error())
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is never called", func() {
				So(len(respBodyReaderMock.ReadCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestDatasetAPI_GetInstance_ResponseBodyErr(t *testing.T) {

	Convey("Given ResponseBodyReader.ReadAll returns an error", t, func() {
		body, _ = json.Marshal(expectedDimensions)
		reader := bytes.NewBuffer(body)
		readCloser := ioutil.NopCloser(reader)

		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return Response(body, 200, nil)
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{
			ReadFunc: func(r io.Reader) ([]byte, error) {
				return nil, errMock
			},
		}

		Convey("When GetInstance is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(instance, ShouldEqual, nil)
				So(err.Error(), ShouldResemble, errors.Wrap(errMock, "unexpected error while attempting to read response body").Error())
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is called 1 time with the expected parameters", func() {
				calls := respBodyReaderMock.ReadCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].R, ShouldResemble, readCloser)
			})
		})
	})
}

func TestDatasetAPI_GetInstance_UnmarshalErr(t *testing.T) {

	Convey("Given unmarshalling the response body returns an error", t, func() {

		body := []byte("INVALID INSTANCE BYTES")
		reader := bytes.NewBuffer(body)
		readCloser := ioutil.NopCloser(reader)

		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return Response(body, 200, nil)
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{
			ReadFunc: func(r io.Reader) ([]byte, error) {
				return body, nil
			},
		}

		Convey("When GetInstance is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(err, ShouldNotBeNil)
				So(strings.Contains(err.Error(), "error while attempting to unmarshal reponse body into model.Instance"), ShouldBeTrue)

				So(instance, ShouldEqual, nil)
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is called 1 time with the expected parameters", func() {
				calls := respBodyReaderMock.ReadCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].R, ShouldResemble, readCloser)
			})
		})
	})
}

func TestGetDimensions(t *testing.T) {

	Convey("Given the client has not been configured", t, func() {
		httpClientMock := &mocks.HTTPClientMock{}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		// Set the host to an empty for this test case.
		datasetAPI := client.DatasetAPI{
			DatasetAPIHost:     "",
			HTTPClient:         httpClientMock,
			ResponseBodyReader: respBodyReaderMock,
		}

		Convey("When the Get is invoked", func() {
			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then no dimenions and the appropriate error are returned.", func() {
				So(err.Error(), ShouldEqual, client.ErrHostEmpty.Error())
				So(dims, ShouldEqual, nil)
			})

			Convey("And HTTPClient.Do and ResponseBodyReader.ReadAll are never called", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 0)
				So(len(respBodyReaderMock.ReadCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given valid client configuration", t, func() {
		httpClientMock := &mocks.HTTPClientMock{}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		Convey("When the client called with a valid instanceID", func() {
			body, _ = json.Marshal(expectedDimensions)
			reader := bytes.NewBuffer(body)
			readCloser := ioutil.NopCloser(reader)

			// set up mocks.
			httpClientMock.DoFunc = func(req *http.Request) (*http.Response, error) {
				return &http.Response{Body: readCloser, StatusCode: 200}, nil
			}

			respBodyReaderMock.ReadFunc = func(r io.Reader) ([]byte, error) {
				return body, nil
			}

			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected response is returned with no error", func() {
				So(dims, ShouldResemble, expectedDimensions.Items)
				So(err, ShouldEqual, nil)
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is called 1 time with the expected parameters", func() {
				calls := respBodyReaderMock.ReadCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].R, ShouldResemble, readCloser)
			})
		})
	})

	Convey("Given an empty instanceID is provided", t, func() {
		httpClientMock := &mocks.HTTPClientMock{}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		Convey("When GetDimensions is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions("")

			Convey("Then the expected error is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err.Error(), ShouldEqual, client.ErrInstanceIDEmpty.Error())
			})
		})
	})

	Convey("Given HTTPClient.Do will return an error", t, func() {
		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, errMock
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		Convey("When GetDimensions is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(dims, ShouldEqual, nil)

				url := fmt.Sprintf(client.GetDimensionsURIFMT, datasetAPI.DatasetAPIHost, instanceID)
				expectedErr := errors.Wrap(errMock, fmt.Sprintf("HTTPClient.Do returned an error when attempting to make request: method: GET, url: %s", url))
				So(err.Error(), ShouldEqual, expectedErr.Error())
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is never called", func() {
				So(len(respBodyReaderMock.ReadCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given HTTPClient.Do returns a non 200 response status", t, func() {
		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return Response([]byte{}, 400, nil)
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		Convey("When GetDimensions is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(dims, ShouldEqual, nil)

				url := fmt.Sprintf(client.GetDimensionsURIFMT, datasetAPI.DatasetAPIHost, instanceID)
				expectedErr := errors.Errorf("incorrect status code: expected: %d, actual: %d, url: %s, method: GET", 200, 400, url)
				So(err.Error(), ShouldEqual, expectedErr.Error())
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is never called", func() {
				So(len(respBodyReaderMock.ReadCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given ResponseBodyReader.ReadAll returns an error", t, func() {
		body, _ = json.Marshal(expectedDimensions)
		reader := bytes.NewBuffer(body)
		readCloser := ioutil.NopCloser(reader)

		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return Response(body, 200, nil)
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{
			ReadFunc: func(r io.Reader) ([]byte, error) {
				return nil, errMock
			},
		}

		Convey("When GetDimensions is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err.Error(), ShouldEqual, errors.Wrap(errMock, "unexpected error while attempting to read response body").Error())
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is called 1 time with the expected parameters", func() {
				calls := respBodyReaderMock.ReadCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].R, ShouldResemble, readCloser)
			})
		})
	})

	Convey("Given unmarshalling the response body returns an error", t, func() {
		body := []byte("INVALID DIMENSIONS BYTES")
		reader := bytes.NewBuffer(body)
		readCloser := ioutil.NopCloser(reader)

		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return Response(body, 200, nil)
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{
			ReadFunc: func(r io.Reader) ([]byte, error) {
				return body, nil
			},
		}

		Convey("When GetDimensions is invoked", func() {
			datasetAPI := client.DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected error response is returned", func() {
				expectedType := reflect.TypeOf((*json.SyntaxError)(nil))
				actualType := reflect.TypeOf(errors.Cause(err))

				So(actualType, ShouldEqual, expectedType)
				So(dims, ShouldEqual, nil)
			})

			Convey("And HTTPClient.Do is called 1 time", func() {
				So(len(httpClientMock.DoCalls()), ShouldEqual, 1)
			})

			Convey("And ResponseBodyReader.ReadAll is called 1 time with the expected parameters", func() {
				calls := respBodyReaderMock.ReadCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].R, ShouldResemble, readCloser)
			})
		})
	})
}

func TestDatasetAPI_PutDimensionNodeID(t *testing.T) {

	Convey("Given datasetAPI.Host has not been set", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := client.DatasetAPI{
			DatasetAPIHost: "",
			HTTPClient:     httpCliMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, client.ErrHostEmpty.Error())
			})

			Convey("And api.HTTPClient.DO is never invoked", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 0)
			})
		})
	})

	Convey("Given no instanceID is provided", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := client.DatasetAPI{
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID("", dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, client.ErrInstanceIDEmpty.Error())
			})

			Convey("And api.HTTPClient.DO is never invoked", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 0)
			})
		})
	})

	Convey("Given no dimension is provided", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := client.DatasetAPI{
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, nil)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension is required but is nil").Error())
			})

			Convey("And api.HTTPClient.DO is never invoked", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension with an empty dimensionID", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := client.DatasetAPI{
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, &model.Dimension{})

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension.id is required but is empty").Error())
			})

			Convey("And api.HTTPClient.DO is never invoked", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 0)
			})
		})
	})

	Convey("Given HTTPClient.Do returns an error", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}
		httpCliMock.DoFunc = func(req *http.Request) (*http.Response, error) {
			return nil, errMock
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				putNodeIDURL := fmt.Sprintf(client.PutDimensionNodeIDURI, datasetAPI.DatasetAPIHost, instanceID, dimensionOne.DimensionID, url.PathEscape(dimensionOne.Option), dimensionOne.NodeID)
				expectedErr := errors.Wrap(errMock, fmt.Sprintf("HTTPClient.Do returned an error when attempting to make request: method: PUT, url: %s", putNodeIDURL))
				So(err.Error(), ShouldEqual, expectedErr.Error())
			})

			Convey("And api.HTTPClient.Do is called 1 time", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 1)
			})

			Convey("And the auth token is set as a request header", func() {
				req := httpCliMock.DoCalls()[0].Req
				actual := req.Header[client.AuthorizationHeader]
				So(actual[0], ShouldEqual, authToken)
			})
		})
	})

	Convey("Given HTTPClient.Do returns a non 200 status", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}
		httpCliMock.DoFunc = func(req *http.Request) (*http.Response, error) {
			return Response([]byte{}, 401, nil)
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				putNodeIDURL := fmt.Sprintf(client.PutDimensionNodeIDURI, datasetAPI.DatasetAPIHost, instanceID, dimensionOne.DimensionID, url.PathEscape(dimensionOne.Option), dimensionOne.NodeID)
				expectedErr := errors.Errorf("incorrect status code: expected: %d, actual: %d, url: %s, method: %s", http.StatusOK, 401, putNodeIDURL, "PUT")
				So(err.Error(), ShouldResemble, expectedErr.Error())
			})

			Convey("And api.HTTPClient.Do is called 1 time", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 1)
			})
		})
	})

	Convey("Given HTTPClient.Do returns a 200 status", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := client.DatasetAPI{
			AuthToken:      authToken,
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}
		httpCliMock.DoFunc = func(req *http.Request) (*http.Response, error) {
			return Response([]byte{}, 200, nil)
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then no error is returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("And api.HTTPClient.Do is called 1 time", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 1)
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
