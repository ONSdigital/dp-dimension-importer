package client

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"net/http"
	"encoding/json"
	"bytes"
	"io/ioutil"
)

var mockHttpGet func(string) (*http.Response, error)
var mockGetInvocations = 0

var d1 = model.Dimension{NodeId: "1111", NodeNamde: "Sex", Value: "Male"}

var d2 = model.Dimension{NodeId: "1112",
	NodeNamde: "Sex",
	Value: "Female",
}

var dimensions = model.Dimensions{
	InstanceId: "123",
	Items: []model.Dimension{d1, d2},
}

var body, _ = json.Marshal(dimensions)

func TestDimensionsClientImpl_Get(t *testing.T) {

	httpGet = mockedResponse(body)

	Convey("Given valid API condiguration", t, func() {
		api := DimensionsClientImpl{
			DimensionsAddr: "http://localhost:8080/dimensions",
		}

		Convey("When the api is called with an empty instanceID value", func() {
			_, err := api.Get("")

			Convey("Then the appropriate error is returned.", func() {
				So(err, ShouldEqual, instanceIDRequiredErr)
			})

			Convey("And no dimensions are returned.", func() {
				So(err, ShouldEqual, instanceIDRequiredErr)
			})

			Convey("And 0 http GET requests are made.", func() {
				So(0, ShouldEqual, mockGetInvocations)
			})

		})
	})
}

func mockedResponse(body []byte) func(string) (*http.Response, error) {
	// Set up the mock.
	reader := bytes.NewReader(body)
	readCloser := ioutil.NopCloser(reader)

	return func(url string) (*http.Response, error) {
		res := http.Response{
			Body: readCloser,
		}
		mockGetInvocations += 1
		return &res, nil
	}
}