package repository

import (
	"errors"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var instance = &model.Instance{InstanceID: instanceID}
var dimension = &model.Dimension{
	DimensionID: "Sex",
	Option:      "Male",
}

var expectedDimension = &model.Dimension{
	DimensionID: "Sex",
	Option:      "Male",
	NodeID:      "1234",
}

func TestDimensionRepository_Insert(t *testing.T) {
	Convey("Given Instance is nil", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}

		repo := DimensionRepository{}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(nil, nil)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(instanceNilErr))
			})

			Convey("And Neo4j.Exec is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})

			Convey("And Neo4j.Query is never called", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an Instance with an empty instanceID", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}

		repo := DimensionRepository{}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(&model.Instance{}, nil)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(instanceIDReqErr))
			})

			Convey("And Neo4j.Exec is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})

			Convey("And Neo4j.Query is never called", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a nil dimension", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}

		repo := DimensionRepository{}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, nil)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(dimensionNilErr))
			})

			Convey("And Neo4j.Exec is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})

			Convey("And Neo4j.Query is never called", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension with an empty dimensionID", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}

		repo := DimensionRepository{}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, &model.Dimension{Option: "10"})

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(dimensionIDRequiredErr))
			})

			Convey("And Neo4j.Exec is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})

			Convey("And Neo4j.Query is never called", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension type that has already been processed", t, func() {
		var nodeID int64 = 1234
		data := [][]interface{}{[]interface{}{nodeID}}

		neoRows := &common.NeoRows{
			Data: data,
		}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(query string, params map[string]interface{}) (*common.NeoRows, error) {
				return neoRows, nil
			},
		}

		repo := DimensionRepository{
			neo4jCli:         neo4jMock,
			constraintsCache: map[string]string{"_" + instanceID + "_" + dimension.DimensionID: ""},
		}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldResemble, expectedDimension)
				So(err, ShouldEqual, nil)
			})

			Convey("And Neo4j.Exec is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})

			Convey("And Neo4j.Query is called 1 time with the expected parameters", func() {
				calls := neo4jMock.QueryCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createDimensionAndInstanceRelStmt, "_"+instanceID+"_Instance", "_"+instanceID+"_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{valueKey: dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given a dimension type that has already been processed", t, func() {
		//var nodeID int64 = 1234
		data := [][]interface{}{[]interface{}{""}}

		neoRows := &common.NeoRows{
			Data: data,
		}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(query string, params map[string]interface{}) (*common.NeoRows, error) {
				return neoRows, nil
			},
		}

		repo := DimensionRepository{
			neo4jCli:         neo4jMock,
			constraintsCache: map[string]string{"_" + instanceID + "_" + dimension.DimensionID: ""},
		}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, errors.New(nodeIDCastErr))
			})

			Convey("And Neo4j.Exec is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})

			Convey("And Neo4j.Query is called 1 time with the expected parameters", func() {
				calls := neo4jMock.QueryCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createDimensionAndInstanceRelStmt, "_"+instanceID+"_Instance", "_"+instanceID+"_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{valueKey: dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given a dimension type that has not already been processed", t, func() {
		var nodeID int64 = 1234
		data := [][]interface{}{[]interface{}{nodeID}}

		neoRows := &common.NeoRows{
			Data: data,
		}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(query string, params map[string]interface{}) (*common.NeoRows, error) {
				return neoRows, nil
			},

			ExecStmtFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		repo := DimensionRepository{
			neo4jCli:         neo4jMock,
			constraintsCache: map[string]string{},
		}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldResemble, expectedDimension)
				So(err, ShouldEqual, nil)
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(uniqueDimConstStmt, "_"+instanceID+"_Sex")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})

			Convey("And Neo4j.Query is called 1 time with the expected parameters", func() {
				calls := neo4jMock.QueryCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createDimensionAndInstanceRelStmt, "_"+instanceID+"_Instance", "_"+instanceID+"_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{valueKey: dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given a create unique constraint returns an error", t, func() {
		var nodeID int64 = 1234
		data := [][]interface{}{[]interface{}{nodeID}}

		neoRows := &common.NeoRows{
			Data: data,
		}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(query string, params map[string]interface{}) (*common.NeoRows, error) {
				return neoRows, nil
			},

			ExecStmtFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, expectedErr
			},
		}

		repo := DimensionRepository{
			neo4jCli:         neo4jMock,
			constraintsCache: map[string]string{},
		}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(uniqueDimConstStmt, "_"+instanceID+"_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})

			Convey("And Neo4j.Query is never invoked", func() {
				calls := neo4jMock.QueryCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given Neo4j.Query returns an error", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(query string, params map[string]interface{}) (*common.NeoRows, error) {
				return nil, expectedErr
			},

			ExecStmtFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		repo := DimensionRepository{
			neo4jCli:         neo4jMock,
			constraintsCache: map[string]string{ "_" + instanceID + "_" +dimension.DimensionID: dimension.DimensionID},
		}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And Neo4j.Exec is never called", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 0)
			})

			Convey("And Neo4j.Query is called 1 time with the expected parameters", func() {
				calls := neo4jMock.QueryCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createDimensionAndInstanceRelStmt, "_"+instanceID+"_Instance", "_123456789_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{valueKey: dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})
}
