package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
)

const (
	host       = "http://localhost:8080"
	instanceID = "1234567890"
	authToken  = "pa55w0rd"
)

var expectedErr = errors.New("BOOM!")
var dimensionOne = &model.Dimension{DimensionID: "666_SEX_MALE", NodeID: "1111", Value: "Male"}
var dimensionTwo = &model.Dimension{DimensionID: "666_SEX_FEMALE", NodeID: "1112", Value: "Female"}

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
		datasetAPI := DatasetAPI{
			DatasetAPIHost:     "",
			HTTPClient:         httpClientMock,
			ResponseBodyReader: respBodyReaderMock,
		}

		Convey("When the GetInstance is called", func() {
			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then a nil instance and the appropriate error is returned.", func() {
				So(instance, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(hostConfigMissingErr))
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

		datasetAPI := DatasetAPI{
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

		datasetAPI := DatasetAPI{
			DatasetAPIHost:     "http://localhost:8080",
			HTTPClient:         httpClientMock,
			ResponseBodyReader: respBodyReaderMock,
		}

		Convey("When GetInstance is invoked", func() {

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error is returned", func() {
				So(instance, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(instanceIDRequiredErr))
			})
		})
	})
}

func TestDatasetAPI_GetInstance_HTTPClientErr(t *testing.T) {

	Convey("Given HTTPClient.Do will return an error", t, func() {

		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, expectedErr
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		Convey("When GetInstance is invoked", func() {
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(instance, ShouldEqual, nil)
				So(err, ShouldResemble, expectedErr)
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
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(instance, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(getInstanceErr))
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
				return nil, expectedErr
			},
		}

		Convey("When GetInstance is invoked", func() {
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(instance, ShouldEqual, nil)
				So(err, ShouldResemble, expectedErr)
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
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			instance, err := datasetAPI.GetInstance(instanceID)

			Convey("Then the expected error response is returned", func() {
				expectedType := reflect.TypeOf((*json.SyntaxError)(nil))
				actualType := reflect.TypeOf(err)

				So(actualType, ShouldEqual, expectedType)
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
		datasetAPI := DatasetAPI{
			DatasetAPIHost:     "",
			HTTPClient:         httpClientMock,
			ResponseBodyReader: respBodyReaderMock,
		}

		Convey("When the Get is invoked", func() {
			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then no dimenions and the appropriate error are returned.", func() {
				So(err, ShouldResemble, errors.New(hostConfigMissingErr))
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

			datasetAPI := DatasetAPI{
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
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions("")

			Convey("Then the expected error is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(instanceIDRequiredErr))
			})
		})
	})

	Convey("Given HTTPClient.Do will return an error", t, func() {
		httpClientMock := &mocks.HTTPClientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, expectedErr
			},
		}
		respBodyReaderMock := &mocks.ResponseBodyReaderMock{}

		Convey("When GetDimensions is invoked", func() {
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldResemble, expectedErr)
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
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(getDimensionsErr))
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
				return nil, expectedErr
			},
		}

		Convey("When GetDimensions is invoked", func() {
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected error response is returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldResemble, expectedErr)
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
			datasetAPI := DatasetAPI{
				DatasetAPIHost:     "http://localhost:8080",
				HTTPClient:         httpClientMock,
				ResponseBodyReader: respBodyReaderMock,
			}

			dims, err := datasetAPI.GetDimensions(instanceID)

			Convey("Then the expected error response is returned", func() {
				expectedType := reflect.TypeOf((*json.SyntaxError)(nil))
				actualType := reflect.TypeOf(err)

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
	// mocks

	Convey("Given datasetAPI.Host has not been set", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost: "",
			HTTPClient:     httpCliMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(hostConfigMissingErr))
			})

			Convey("And api.HTTPClient.DO is never invoked", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 0)
			})
		})
	})

	Convey("Given no instanceID is provided", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID("", dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceIDRequiredErr))
			})

			Convey("And api.HTTPClient.DO is never invoked", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 0)
			})
		})
	})

	Convey("Given no dimension is provided", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, nil)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(dimensionNilErr))
			})

			Convey("And api.HTTPClient.DO is never invoked", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension with an empty dimensionID", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost: host,
			HTTPClient:     httpCliMock,
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, &model.Dimension{})

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(dimensionIDReqErr))
			})

			Convey("And api.HTTPClient.DO is never invoked", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 0)
			})
		})
	})

	Convey("Given HTTPClient.Do returns an error", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost:      host,
			HTTPClient:          httpCliMock,
			DatasetAPIAuthToken: authToken,
		}
		httpCliMock.DoFunc = func(req *http.Request) (*http.Response, error) {
			return nil, expectedErr
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And api.HTTPClient.Do is called 1 time", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 1)
			})

			Convey("And the auth token is set as a request header", func() {
				req := httpCliMock.DoCalls()[0].Req
				actual := req.Header[authTokenHeader]
				So(actual[0], ShouldEqual, authToken)
			})
		})
	})

	Convey("Given HTTPClient.Do returns an 401 status", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost:      host,
			HTTPClient:          httpCliMock,
			DatasetAPIAuthToken: authToken,
		}
		httpCliMock.DoFunc = func(req *http.Request) (*http.Response, error) {
			return Response([]byte{}, 401, nil)
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(unauthorisedResponse))
			})

			Convey("And api.HTTPClient.Do is called 1 time", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 1)
			})
		})
	})

	Convey("Given HTTPClient.Do returns an 403 status", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost:      host,
			HTTPClient:          httpCliMock,
			DatasetAPIAuthToken: authToken,
		}
		httpCliMock.DoFunc = func(req *http.Request) (*http.Response, error) {
			return Response([]byte{}, 403, nil)
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(forbiddenResponse))
			})

			Convey("And api.HTTPClient.Do is called 1 time", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 1)
			})
		})
	})

	Convey("Given HTTPClient.Do returns an unexpected status", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost:      host,
			HTTPClient:          httpCliMock,
			DatasetAPIAuthToken: authToken,
		}
		httpCliMock.DoFunc = func(req *http.Request) (*http.Response, error) {
			return Response([]byte{}, 500, nil)
		}

		Convey("When PutDimensionNodeID is called", func() {
			err := datasetAPI.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(putDimNodeIDErr))
			})

			Convey("And api.HTTPClient.Do is called 1 time", func() {
				calls := len(httpCliMock.DoCalls())
				So(calls, ShouldEqual, 1)
			})
		})
	})

	Convey("Given HTTPClient.Do returns a 200 status", t, func() {
		httpCliMock := &mocks.HTTPClientMock{}

		datasetAPI := DatasetAPI{
			DatasetAPIHost:      host,
			HTTPClient:          httpCliMock,
			DatasetAPIAuthToken: authToken,
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
