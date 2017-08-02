package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/model"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"reflect"
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/logging"
)

const host = "http://localhost:8080"
const instanceID = "1234567890"

var dimensionOne = &model.Dimension{Dimension_ID: "666_SEX_MALE", NodeId: "1111", Value: "Male"}
var dimensionTwo = &model.Dimension{Dimension_ID: "666_SEX_FEMALE", NodeId: "1112", Value: "Female"}
var expectedDimensions = []*model.Dimension{dimensionOne, dimensionTwo}

var body []byte

var importAPI = ImportAPI{}

func TestGetDimensions(t *testing.T) {

	logging.Init(logging.DebugLevel)

	Convey("Given the client has not been configured", t, func() {
		mock := mockIt(&GetDimensionsMock{
			StatusCode:   200,
			Error:        nil,
			Body:         nil,
			URLParam:     fmt.Sprintf(getDimensionsURIFMT, host, instanceID),
			Reader:       ioutil.ReadAll,
			ReaderCount:  0,
			HTTPGetCount: 0,
		})

		// Set the host to an empty for this test case.
		Host = ""

		Convey("When the Get is invoked", func() {
			dims, err := importAPI.GetDimensions(instanceID)

			Convey("Then no dimenions and the appropriate error are returned.", func() {
				So(err, ShouldResemble, errors.New(hostConfigMissingErr))
				So(dims, ShouldEqual, nil)
			})

			Convey("And 0 HTTP GET requests are made to the Import API", func() {
				So(mock.HTTPGetCount, ShouldEqual, 0)
			})
		})
	})

	Convey("Given valid client configuration", t, func() {

		Convey("When the client called with a valid instanceID", func() {
			body, _ = json.Marshal(expectedDimensions)

			mock := mockIt(&GetDimensionsMock{
				StatusCode:   200,
				Error:        nil,
				Body:         dimensionsBytes(expectedDimensions),
				URLParam:     fmt.Sprintf(getDimensionsURIFMT, host, instanceID),
				Reader:       ioutil.ReadAll,
				ReaderCount:  0,
				HTTPGetCount: 0,
			})

			dims, err := importAPI.GetDimensions(instanceID)

			Convey("Then the expected dimensions are returned", func() {
				So(dims, ShouldResemble, expectedDimensions)
			})

			Convey("And a single request is made to the Import API to get the dimensions", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
				So(fmt.Sprintf(getDimensionsURIFMT, host, instanceID), ShouldEqual, mock.URLParam)
				So(mock.ReaderCount, ShouldEqual, 1)
			})

			Convey("And no error is returned", func() {
				So(nil, ShouldEqual, err)
			})
		})

		Convey("When the client is called with an empty instanceID value", func() {
			mock := mockIt(&GetDimensionsMock{
				StatusCode:   0,
				Body:         nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := importAPI.GetDimensions("")

			Convey("Then an appropriate error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceIDRequiredErr))
			})

			Convey("And no dimensions are returned", func() {
				So(dims, ShouldEqual, nil)
			})

			Convey("And 0 http GET requests are made to the Import API", func() {
				So(0, ShouldEqual, mock.HTTPGetCount)
				So(0, ShouldEqual, mock.ReaderCount)
				So(dims, ShouldEqual, nil)
			})
		})

		Convey("When the client is invoked and returns a 404 response status", func() {
			mock := mockIt(&GetDimensionsMock{
				StatusCode:   404,
				Body:         nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := importAPI.GetDimensions(instanceID)

			Convey("Then 1 http GET request is made to the Import API", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
			})

			Convey("And no dimensions are returned along with the appropriate error", func() {
				So(err, ShouldResemble, errors.New(getDimensionsErr))
				So(dims, ShouldEqual, nil)
			})

			Convey("And the response body is never read", func() {
				So(0, ShouldEqual, mock.ReaderCount)
			})
		})

		Convey("When the client is invoked and returns a 500 response status", func() {
			mock := mockIt(&GetDimensionsMock{
				StatusCode:   500,
				Body:         nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := importAPI.GetDimensions(instanceID)

			Convey("Then 1 http GET request is made to the Import API", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
			})

			Convey("And no dimensions are returned along with the appropriate error", func() {
				So(err, ShouldResemble, errors.New(getDimensionsErr))
				So(dims, ShouldEqual, nil)
			})

			Convey("And the response body is never read.", func() {
				So(0, ShouldEqual, mock.ReaderCount)
			})
		})

		Convey("When the client returns an unexpected HTTP status code", func() {
			mock := mockIt(&GetDimensionsMock{
				StatusCode:   503,
				Body:         nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := importAPI.GetDimensions(instanceID)

			Convey("Then 1 http GET request is made to the Import API", func() {
				So(mock.HTTPGetCount, ShouldEqual, 1)
			})

			Convey("And the response body is never read", func() {
				So(1, ShouldEqual, mock.HTTPGetCount)
			})

			Convey("And no dimensions and the appropriate error are returned", func() {
				So(dims, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(getDimensionsErr))
			})
		})

		Convey("When the client is invoked and the HTTP request returns an error", func() {
			expectedErr := errors.New("Unexpected error")

			mock := mockIt(&GetDimensionsMock{
				StatusCode:   500,
				Body:         nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        expectedErr,
				Reader:       ioutil.ReadAll,
			})

			dims, err := importAPI.GetDimensions(instanceID)

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
			junkBody := []byte("This is not a dimension")

			mock := mockIt(&GetDimensionsMock{
				StatusCode:   200,
				Body:         junkBody,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader:       ioutil.ReadAll,
			})

			dims, err := importAPI.GetDimensions(instanceID)

			Convey("Then 1 HTTP GET request was made to the Import API", func() {
				So(mock.HTTPGetCount, ShouldEqual, 1)
			})

			Convey("And the response body was read once.", func() {
				So(mock.ReaderCount, ShouldEqual, 1)
			})

			Convey("And no dimensions are returned along with the appropriate error.", func() {
				expectedType := reflect.TypeOf((*json.SyntaxError)(nil))
				actualType := reflect.TypeOf(err)

				So(actualType, ShouldEqual, expectedType)
				So(dims, ShouldEqual, nil)
			})

		})

		Convey("When reading the response body return an error", func() {
			readBodyErr := errors.New("Read body error")

			mock := mockIt(&GetDimensionsMock{
				StatusCode:   200,
				Body:         nil,
				HTTPGetCount: 0,
				ReaderCount:  0,
				Error:        nil,
				Reader: func(io.Reader) ([]byte, error) {
					return nil, readBodyErr
				},
			})

			dims, err := importAPI.GetDimensions(instanceID)

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

func TestImportAPI_SetDimensionNodeID(t *testing.T) {

	Convey("Given no Host has been set", t, func() {
		Host = ""
		httpCliMock := &httpClientMock{}
		httpCli = httpCliMock

		Convey("When PutDimensionNodeID is invoked", func() {
			api := ImportAPI{}
			err := api.PutDimensionNodeID(instanceID, nil)

			Convey("Then a a missing config error is returned", func() {
				So(err, ShouldResemble, errors.New(hostConfigMissingErr))
			})

			Convey("And httpClient is never invoked", func() {
				So(httpCliMock.invocations, ShouldEqual, 0)
			})
		})
	})

	Convey("Given the client has been correctly configured.", t, func() {
		Host = "http://localhost:8080/test"
		httpCliMock := &httpClientMock{}
		httpCli = httpCliMock

		Convey("When PutDimensionNodeID is invoked with an empty instanceID", func() {
			api := ImportAPI{}
			err := api.PutDimensionNodeID("", nil)

			Convey("Then a a missing config error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceIDRequiredErr))
			})

			Convey("And httpClient is never invoked", func() {
				So(httpCliMock.invocations, ShouldEqual, 0)
			})
		})

		Convey("When PutDimensionNodeID is invoked with an empty dimension", func() {
			httpCliMock := &httpClientMock{}
			httpCli = httpCliMock

			api := ImportAPI{}
			err := api.PutDimensionNodeID("1234", nil)

			Convey("Then the expected error is returned.", func() {
				So(err, ShouldResemble, errors.New(dimensionNilErr))
			})

			Convey("And httpClient is never invoked", func() {
				So(httpCliMock.invocations, ShouldEqual, 0)
			})
		})

		Convey("When httpClient returns an error", func() {
			expected := errors.New("HttpCli error")

			httpCliMock := &httpClientMock{}
			httpCli = httpCliMock
			httpCliMock.WhenDoReturn(nil, expected)

			api := ImportAPI{}
			err := api.PutDimensionNodeID("1234", dimensionOne)

			Convey("Then the expected error is returned.", func() {
				So(err, ShouldResemble, expected)
			})

			Convey("And httpClient is invoked 1 time", func() {
				So(httpCliMock.invocations, ShouldEqual, 1)
			})
		})

		Convey("When http client returns a 200 status", func() {
			httpCliMock := &httpClientMock{}
			httpCli = httpCliMock
			httpCliMock.WhenDoReturn(MockResponse(200), nil)

			api := ImportAPI{}
			err := api.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then httpClient has been invoked 1 time", func() {
				So(httpCliMock.invocations, ShouldEqual, 1)
			})

			Convey("And no error is returned", func() {
				So(err, ShouldEqual, nil)
			})
		})

		Convey("When http client returns a 404 status", func() {
			httpCliMock := &httpClientMock{}
			httpCli = httpCliMock
			httpCliMock.WhenDoReturn(MockResponse(404), nil)

			api := ImportAPI{}
			err := api.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then httpClient has been invoked 1 time", func() {
				So(httpCliMock.invocations, ShouldEqual, 1)
			})

			Convey("And the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(putDimNodeIDErr))
			})
		})

		Convey("When http client return a 500 status", func() {
			httpCliMock := &httpClientMock{}
			httpCli = httpCliMock
			httpCliMock.WhenDoReturn(MockResponse(500), nil)

			api := ImportAPI{}
			err := api.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then httpClient has been invoked 1 time", func() {
				So(httpCliMock.invocations, ShouldEqual, 1)
			})

			Convey("And the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(putDimNodeIDErr))
			})
		})

		Convey("When http client returns an unexpected HTTP status", func() {
			httpCliMock := &httpClientMock{}
			httpCli = httpCliMock
			httpCliMock.WhenDoReturn(MockResponse(503), nil)

			api := ImportAPI{}
			err := api.PutDimensionNodeID(instanceID, dimensionOne)

			Convey("Then httpClient has been invoked 1 time", func() {
				So(httpCliMock.invocations, ShouldEqual, 1)
			})

			Convey("And the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(putDimNodeIDErr))
			})
		})
	})
}

func mockIt(m *GetDimensionsMock) *GetDimensionsMock {
	m.ReaderCount = 0
	m.HTTPGetCount = 0

	httpGet = m.httpGet()
	respBodyReader = m.MockReader
	Host = host
	return m
}

type GetDimensionsMock struct {
	StatusCode   int
	HTTPGetCount int
	ReaderCount  int
	Body         []byte
	Error        error
	URLParam     string
	Reader       func(reader io.Reader) ([]byte, error)
}

func (m *GetDimensionsMock) httpGet() func(string) (*http.Response, error) {
	m.HTTPGetCount = 0
	reader := bytes.NewReader(m.Body)
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

func (m *GetDimensionsMock) MockReader(reader io.Reader) ([]byte, error) {
	m.ReaderCount++
	return m.Reader(reader)
}

func dimensionsBytes(d []*model.Dimension) []byte {
	body, _ = json.Marshal(expectedDimensions)
	return body
}

type httpClientMock struct {
	response    *http.Response
	err         error
	invocations int
}

func (cli *httpClientMock) WhenDoReturn(r *http.Response, err error) {
	cli.response = r
	cli.err = err
}

func (cli *httpClientMock) Do(req *http.Request) (*http.Response, error) {
	cli.invocations = cli.invocations + 1
	return cli.response, cli.err
}

func MockResponse(statusCode int) *http.Response {
	reader := bytes.NewReader(make([]byte, 0))
	readCloser := ioutil.NopCloser(reader)
	return &http.Response{Body: readCloser, StatusCode: statusCode}
}
