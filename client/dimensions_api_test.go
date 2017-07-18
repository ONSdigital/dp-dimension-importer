package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/model"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
)

const host = "http://localhost:8080"
const instanceID = "1234567890"

var dimensionOne = model.Dimension{NodeId: "1111", NodeNamde: "Sex", Value: "Male"}
var dimensionTwo = model.Dimension{NodeId: "1112", NodeNamde: "Sex", Value: "Female"}
var expectedDimensions = &model.Dimensions{InstanceId: "123", Items: []model.Dimension{dimensionOne, dimensionTwo}}

var body []byte

func TestDimensionsClientImpl_Get(t *testing.T) {

	Convey("Given the client has not been configured", t, func() {
		mock := mockIt(&Mock{
			StatusCode:   200,
			Error:        nil,
			Dimensions:   expectedDimensions,
			URLParam:     fmt.Sprintf(dimensionsHostFMT, host, instanceID),
			Reader:       ioutil.ReadAll,
			ReaderCount:  0,
			HTTPGetCount: 0,
		})

		client := DimensionsClientImpl{}

		Convey("When the Get is invoked", func() {
			dims, err := client.Get(instanceID)

			Convey("Then no dimenions and the appropriate error are returned.", func() {
				So(err, ShouldResemble, missingConfigErr)
				So(dims, ShouldEqual, nil)
			})

			Convey("And 0 HTTP GET requests are made to the Import API", func() {
				So(mock.HTTPGetCount, ShouldEqual, 0)
			})
		})
	})

	Convey("Given valid client configuration", t, func() {
		client := DimensionsClientImpl{Host: host}
		unmarshal = json.Unmarshal

		Convey("When the client called with a valid instanceID", func() {
			body, _ = json.Marshal(expectedDimensions)

			mock := mockIt(&Mock{
				StatusCode:   200,
				Error:        nil,
				Dimensions:   expectedDimensions,
				URLParam:     fmt.Sprintf(dimensionsHostFMT, host, instanceID),
				Reader:       ioutil.ReadAll,
				ReaderCount:  0,
				HTTPGetCount: 0,
			})

			dims, err := client.Get(instanceID)

			Convey("Then the expected dimensions are returned", func() {
				So(dims, ShouldResemble, mock.Dimensions)
			})

			Convey("And a single request is made to the Import API to get the dimensions", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
				So(fmt.Sprintf(dimensionsHostFMT, host, instanceID), ShouldEqual, mock.URLParam)
				So(mock.ReaderCount, ShouldEqual, 1)
			})

			Convey("And no error is returned", func() {
				So(nil, ShouldEqual, err)
			})
		})

		Convey("When the client is called with an empty instanceID value", func() {
			mock := mockIt(&Mock{
				StatusCode:   0,
				Dimensions:   nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := client.Get("")

			Convey("Then an appropriate error is returned", func() {
				So(err, ShouldEqual, instanceIDRequiredErr)
			})

			Convey("And no dimensions are returned", func() {
				So(err, ShouldEqual, instanceIDRequiredErr)
			})

			Convey("And 0 http GET requests are made to the Import API", func() {
				So(0, ShouldEqual, mock.HTTPGetCount)
				So(0, ShouldEqual, mock.ReaderCount)
				So(dims, ShouldEqual, nil)
			})
		})

		Convey("When the client is invoked and returns a 404 response status", func() {
			mock := mockIt(&Mock{
				StatusCode:   404,
				Dimensions:   nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := client.Get(instanceID)

			Convey("Then 1 http GET request is made to the Import API", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
			})

			Convey("And no dimensions are returned along with the appropriate error", func() {
				So(err, ShouldResemble, instanceIDNotFoundErr)
				So(dims, ShouldEqual, nil)
			})

			Convey("And the response body is never read", func() {
				So(0, ShouldEqual, mock.ReaderCount)
			})
		})

		Convey("When the client is invoked and returns a 500 response status", func() {
			mock := mockIt(&Mock{
				StatusCode:   500,
				Dimensions:   nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := client.Get(instanceID)

			Convey("Then 1 http GET request is made to the Import API", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
			})

			Convey("And no dimensions are returned along with the appropriate error", func() {
				So(err, ShouldResemble, internalServerErr)
				So(dims, ShouldEqual, nil)
			})

			Convey("And the response body is never read.", func() {
				So(0, ShouldEqual, mock.ReaderCount)
			})
		})

		Convey("When the client returns an unexpected HTTP status code", func() {
			mock := mockIt(&Mock{
				StatusCode:   503,
				Dimensions:   nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := client.Get(instanceID)

			Convey("Then 1 http GET request is made to the Import API", func() {
				So(mock.HTTPGetCount, ShouldEqual, 1)
			})

			Convey("And the response body is never read", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
			})

			Convey("And no dimensions and the appropriate error are returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldEqual, unexpectedErr)
			})
		})

		Convey("When the client is invoked and the HTTP request returns an error", func() {
			expectedErr := errors.New("Unexpected error")

			mock := mockIt(&Mock{
				StatusCode:   500,
				Dimensions:   nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        expectedErr,
				Reader:       ioutil.ReadAll,
			})

			dims, err := client.Get(instanceID)

			Convey("Then 1 HTTP GET request is made to the Import API", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
			})

			Convey("And no dimensions are returned along with the appropriate error.", func() {
				So(err, ShouldResemble, expectedErr)
				So(dims, ShouldEqual, nil)
			})

			Convey("And the response body is never read.", func() {
				So(mock.ReaderCount, ShouldEqual, 0)
			})
		})

		Convey("When unmarshalling the response returns an error", func() {
			unmarshalErr := errors.New("Unmarshal error")

			mock := mockIt(&Mock{
				StatusCode:   200,
				Dimensions:   nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			unmarshalCount := 0
			unmarshal = func([]byte, interface{}) error {
				unmarshalCount++
				return unmarshalErr
			}

			dims, err := client.Get(instanceID)

			Convey("Then 1 HTTP GET request was made to the Import API", func() {
				So(mock.HTTPGetCount, ShouldEqual, 1)
			})

			Convey("And the response body was read once.", func() {
				So(mock.ReaderCount, ShouldEqual, 1)
			})

			Convey("And the body was unmarshalled once.", func() {
				So(unmarshalCount, ShouldEqual, 1)
			})

			Convey("And no dimensions are returned along with the appropriate error.", func() {
				So(err, ShouldResemble, unmarshalErr)
				So(dims, ShouldEqual, nil)
			})

		})

		Convey("When reading the response body return an error", func() {
			readBodyErr := errors.New("Read body error")

			mock := mockIt(&Mock{
				StatusCode:   200,
				Dimensions:   nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader: func(io.Reader) ([]byte, error) {
					return nil, readBodyErr
				},
			})

			dims, err := client.Get(instanceID)

			Convey("Then 1 HTTP GET request was made to the Import API", func() {
				So(mock.HTTPGetCount, ShouldEqual, 1)
			})

			Convey("And the response body was read once.", func() {
				So(mock.ReaderCount, ShouldEqual, 1)
			})

			Convey("And no dimensions are returned along with the appropriate error.", func() {
				So(err, ShouldResemble, readBodyErr)
				So(dims, ShouldEqual, nil)
			})
		})
	})
}

func mockIt(m *Mock) *Mock {
	m.ReaderCount = 0
	m.HTTPGetCount = 0

	httpGet = m.httpGet()
	respBodyReader = m.MockReader
	return m
}

type Mock struct {
	StatusCode   int
	HTTPGetCount int
	ReaderCount  int
	Dimensions   *model.Dimensions
	Error        error
	URLParam     string
	Reader       func(reader io.Reader) ([]byte, error)
}

func (m *Mock) httpGet() func(string) (*http.Response, error) {
	m.HTTPGetCount = 0

	if m.Dimensions == nil {
		body = make([]byte, 0)
	} else {
		body, _ = json.Marshal(m.Dimensions)
	}

	reader := bytes.NewReader(body)
	readCloser := ioutil.NopCloser(reader)

	return func(url string) (*http.Response, error) {
		res := http.Response{
			Body:       readCloser,
			StatusCode: m.StatusCode,
		}
		m.HTTPGetCount++
		return &res, m.Error
	}
}

func (m *Mock) MockReader(reader io.Reader) ([]byte, error) {
	m.ReaderCount++
	return m.Reader(reader)
}
