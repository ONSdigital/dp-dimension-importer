package repository

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"fmt"
)

var expectedErr = errors.New("I am Expected")
var instanceID = "123456789"

func TestInstanceRepository_AddDimensions(t *testing.T) {

	Convey("Given a nil Instance", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}
		repo := InstanceRepository{
			Neo4j: neo4jMock,
		}

		Convey("When AddDimensions is called", func() {
			err := repo.AddDimensions(nil)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceNilErr))
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an Instance with an empty InstanceID", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}
		repo := InstanceRepository{
			Neo4j: neo4jMock,
		}

		instance := &model.Instance{}

		Convey("When AddDimensions is called", func() {
			err := repo.AddDimensions(instance)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceIDReqErr))
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given Neo4j.ExecStmt returns an error", t, func() {
		dimensionNames := []interface{}{"one", "two", "three", "four"}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, expectedErr
			},
		}

		repo := InstanceRepository{
			Neo4j: neo4jMock,
		}

		instance := &model.Instance{
			InstanceID: instanceID,
			Dimensions: dimensionNames,
		}

		Convey("When AddDimensions is called", func() {
			err := repo.AddDimensions(instance)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				instanceLabel := fmt.Sprintf(instanceLabelFmt, instanceID)
				expectedStmt := fmt.Sprintf(addInstanceDimensionsStmt, instanceLabel)
				So(calls[0].Query, ShouldEqual, expectedStmt)

				expectedParams := map[string]interface{}{dimensionsList: dimensionNames}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given Neo4j.ExecStmt does not return an error", t, func() {
		dimensionNames := []interface{}{"one", "two", "three", "four"}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		repo := InstanceRepository{
			Neo4j: neo4jMock,
		}

		instance := &model.Instance{
			InstanceID: instanceID,
			Dimensions: dimensionNames,
		}

		Convey("When AddDimensions is called", func() {
			err := repo.AddDimensions(instance)

			Convey("Then no error is returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				instanceLabel := fmt.Sprintf(instanceLabelFmt, instanceID)
				expectedStmt := fmt.Sprintf(addInstanceDimensionsStmt, instanceLabel)
				So(calls[0].Query, ShouldEqual, expectedStmt)

				expectedParams := map[string]interface{}{dimensionsList: dimensionNames}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})
}

func TestInstanceRepository_Create(t *testing.T) {
	Convey("Given no instanceID is provided", t, func() {
		instance := &model.Instance{}

		neo4jMock := &mocks.Neo4jClientMock{}

		repo := InstanceRepository{
			Neo4j: neo4jMock,
		}

		Convey("When Create is invoked", func() {
			err := repo.Create(instance)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceIDReqErr))
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a nil Onstance is provided", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}

		repo := InstanceRepository{
			Neo4j: neo4jMock,
		}

		Convey("When Create is invoked", func() {
			err := repo.Create(nil)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceNilErr))
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a Neo4j.ExecStmt returns an error", t, func() {
		instance := &model.Instance{InstanceID: instanceID}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, expectedErr
			},
		}

		repo := InstanceRepository{
			Neo4j: neo4jMock,
		}

		Convey("When Create is invoked", func() {
			err := repo.Create(instance)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createInstanceStmt, "_"+instanceID+"_Instance")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})

	Convey("Given a Neo4j.ExecStmt returns no error", t, func() {
		instance := &model.Instance{InstanceID: instanceID}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		repo := InstanceRepository{
			Neo4j: neo4jMock,
		}

		Convey("When Create is invoked", func() {
			err := repo.Create(instance)

			Convey("Then no error is returned", func() {
				So(err, ShouldResemble, nil)
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createInstanceStmt, "_"+instanceID+"_Instance")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})
}