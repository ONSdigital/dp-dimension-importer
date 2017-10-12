package repository

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
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

			Convey("And there are no interactions with neo4j", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
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

			Convey("And there are no interactions with neo4j", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
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

			Convey("And there are no interactions with neo4j", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
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

			Convey("And there are no interactions with neo4j", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension type that has already been processed", t, func() {
		var nodeID int64 = 1234
		data := [][]interface{}{[]interface{}{nodeID}}
		neoRows := &common.NeoRows{
			Data: data,
		}

		connMock := &mocks.NeoConnMock{}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
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

			Convey("And neo4j.Query is called 1 time with the expected parameters", func() {
				calls := neo4jMock.QueryCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createDimensionAndInstanceRelStmt, "_"+instanceID+"_Instance", "_"+instanceID+"_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{valueKey: dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension type that has already been processed", t, func() {
		data := [][]interface{}{[]interface{}{""}}

		neoRows := &common.NeoRows{
			Data: data,
		}

		connMock := &mocks.NeoConnMock{}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
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

			Convey("And neo4j.Query is called 1 time with the expected parameters", func() {
				calls := neo4jMock.QueryCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createDimensionAndInstanceRelStmt, "_"+instanceID+"_Instance", "_"+instanceID+"_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{valueKey: dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension type that has not already been processed", t, func() {
		var nodeID int64 = 1234
		data := [][]interface{}{[]interface{}{nodeID}}

		neoRows := &common.NeoRows{
			Data: data,
		}

		connMock := &mocks.NeoConnMock{}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
				return neoRows, nil
			},
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		repo := DimensionRepository{
			neo4jCli:         neo4jMock,
			constraintsCache: map[string]string{},
			conn:             connMock,
		}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldResemble, expectedDimension)
				So(err, ShouldEqual, nil)
			})

			Convey("And neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(uniqueDimConstStmt, "_"+instanceID+"_Sex")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})

			Convey("And neo4j.Query is called 1 time with the expected parameters", func() {
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

		connMock := &mocks.NeoConnMock{}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
				return neoRows, nil
			},
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, expectedErr
			},
		}

		repo := DimensionRepository{
			neo4jCli:         neo4jMock,
			constraintsCache: map[string]string{},
			conn:             connMock,
		}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(uniqueDimConstStmt, "_"+instanceID+"_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})

			Convey("And there is no other calls to neo4j", func() {
				So(len(neo4jMock.QueryCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given neo4j.Query returns an error", t, func() {
		connMock := &mocks.NeoConnMock{}

		neo4jMock := &mocks.Neo4jClientMock{
			QueryFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
				return nil, expectedErr
			},
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		repo := DimensionRepository{
			neo4jCli:         neo4jMock,
			constraintsCache: map[string]string{"_" + instanceID + "_" + dimension.DimensionID: dimension.DimensionID},
		}

		Convey("When Insert is invoked", func() {
			dim, err := repo.Insert(instance, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And neo4j.Query is called 1 time with the expected parameters", func() {
				calls := neo4jMock.QueryCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createDimensionAndInstanceRelStmt, "_"+instanceID+"_Instance", "_123456789_"+dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{valueKey: dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})

			Convey("And there is no other calls to neo4j", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestNewDimensionRepository(t *testing.T) {
	Convey("Given valid parameters", t, func() {
		connMock := &mocks.NeoConnMock{}
		neo4jCliMock := &mocks.Neo4jClientMock{}
		connectionPool := &mocks.NeoDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return connMock, nil
			},
		}

		Convey("When NewDimensionRepository is called", func() {
			repo, err := NewDimensionRepository(connectionPool, neo4jCliMock)

			Convey("Then the expected repository is returned with no error", func() {
				So(repo.conn, ShouldEqual, connMock)
				So(repo.neo4jCli, ShouldEqual, neo4jCliMock)
				So(err, ShouldBeNil)
			})

			Convey("And connectionPool.OpenPool is called 1 time", func() {
				So(len(connectionPool.OpenPoolCalls()), ShouldEqual, 1)
			})
		})

		Convey("When connectionPoo.OpenPool returns an error", func() {
			connectionPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return nil, errors.New("Conn pool error")
			}

			repo, err := NewDimensionRepository(connectionPool, neo4jCliMock)
			Convey("Then the error is propagated", func() {
				So(err, ShouldResemble, errors.New("Conn pool error"))
				So(repo, ShouldBeNil)
			})
		})
	})
}

func TestDimensionRepository_Close(t *testing.T) {
	Convey("Given DimensionRepository.conn is not nil", t, func() {
		connMock := &mocks.NeoConnMock{
			CloseFunc: func() error {
				return nil
			},
		}
		repo := DimensionRepository{conn: connMock}

		Convey("When Close is invoked", func() {
			repo.Close()

			Convey("Then conn.Close is called 1 time", func() {
				So(len(connMock.CloseCalls()), ShouldEqual, 1)
			})
		})
	})
}
