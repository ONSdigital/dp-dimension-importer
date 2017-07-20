package client

/*

import (
	"github.com/ONSdigital/dp-dimension-importer/model"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var neo GraphDBClient
var dimension model.Dimension = model.Dimension{
	NodeName: "Sex",
	Value:    "Male",
}

func TestNeo4jClientImpl_Create(t *testing.T) {
	neo = &Neo4jClient{}

	Convey("Given the client has been correctly configured", t, func() {

		Convey("When BatchInsert is invoked with a valid dimension", func() {
			instanceID := "999999"

			neo.BatchInsert(instanceID, dimension)
		})

		Convey("When BatchInsert is invoked with an empty instanceID", func() {
			err := neo.BatchInsert("", dimension)

			Convey("Then am error is returned stating the instanceID was empty", func() {
				So(err, ShouldEqual, instanceIDRequiredErr)
			})
		})

		Convey("When BatchInsert is invoked with an empty dimension", func() {
			dimension = model.Dimension{}
			instanceID := "999666"
			err := neo.BatchInsert(instanceID, dimension)

			Convey("Then am error is returned stating the dimension invalid as it was empty", func() {
				So(err, ShouldEqual, errDimensionEmpty)
			})
		})

		Convey("When BatchInsert is invoked with an dimension with no name value", func() {
			dimension = model.Dimension{Value: "Male"}
			instanceID := "999666"
			err := neo.BatchInsert(instanceID, dimension)

			Convey("Then am error is returned stating the dimension invalid as name was empty", func() {
				So(err, ShouldEqual, errDimensionNameRequired)
			})
		})

		Convey("When BatchInsert is invoked with an dimension with no value", func() {
			dimension = model.Dimension{NodeName: "Geography"}
			instanceID := "999666"
			err := neo.BatchInsert(instanceID, dimension)

			Convey("Then am error is returned stating the dimension invalid as value was empty", func() {
				So(err, ShouldEqual, errDimensionValueRequired)
			})
		})

	})
}
*/
