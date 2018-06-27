package repository

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var errorMock = errors.New("I am Expected")
var instanceID = "123456789"

func TestInstanceRepository_AddDimensions(t *testing.T) {

	Convey("Given a nil Instance", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}
		connMock := &mocks.NeoConnMock{}
		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When AddDimensions is called", func() {
			err := repo.AddDimensions(nil)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance is required but was nil").Error())
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an Instance with an empty InstanceID", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}
		connMock := &mocks.NeoConnMock{}
		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		instance := &model.Instance{}

		Convey("When AddDimensions is called", func() {
			err := repo.AddDimensions(instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance id is required but was empty").Error())
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				So(len(neo4jMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given Neo4j.ExecStmt returns an error", t, func() {
		dimensionNames := []interface{}{"one", "two", "three", "four"}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}
		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		instance := &model.Instance{
			InstanceID: instanceID,
			Dimensions: dimensionNames,
		}

		Convey("When AddDimensions is called", func() {
			err := repo.AddDimensions(instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.ExecStmt returned an error").Error())
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				instanceLabel := fmt.Sprintf(instanceLabelFmt, instanceID)
				expectedStmt := fmt.Sprintf(addInstanceDimensionsStmt, instanceLabel)
				So(calls[0].Query, ShouldEqual, expectedStmt)

				expectedParams := map[string]interface{}{"dimensions_list": dimensionNames}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given Neo4j.ExecStmt does not return an error", t, func() {
		dimensionNames := []interface{}{"one", "two", "three", "four"}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
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

				expectedParams := map[string]interface{}{"dimensions_list": dimensionNames}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})
}

func TestInstanceRepository_Create(t *testing.T) {
	Convey("Given no instanceID is provided", t, func() {
		instance := &model.Instance{}

		neo4jMock := &mocks.Neo4jClientMock{}
		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When Create is invoked", func() {
			err := repo.Create(instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance id is required but was empty").Error())
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a nil instance is provided", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}
		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When Create is invoked", func() {
			err := repo.Create(nil)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance is required but was nil").Error())
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a Neo4j.ExecStmt returns an error", t, func() {
		instance := &model.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}

		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When Create is invoked", func() {
			err := repo.Create(instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.ExecStmt returned an error").Error())
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createInstanceStmt, "_"+instanceID+"_Instance", strings.Join(instance.CSVHeader, ","))
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})

	Convey("Given a Neo4j.ExecStmt returns no error", t, func() {
		instance := &model.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When Create is invoked", func() {
			err := repo.Create(instance)

			Convey("Then no error is returned", func() {
				So(err, ShouldResemble, nil)
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createInstanceStmt, "_"+instanceID+"_Instance", strings.Join(instance.CSVHeader, ","))
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})
}

func TestInstanceRepository_CreateCodeRelationship(t *testing.T) {

	code := "123"
	instance := &model.Instance{
		InstanceID: instanceID,
	}

	Convey("Given no instanceID is provided", t, func() {
		instance := &model.Instance{}

		neo4jMock := &mocks.Neo4jClientMock{}
		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := repo.CreateCodeRelationship(instance, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance id is required but was empty").Error())
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a nil instance is provided", t, func() {
		neo4jMock := &mocks.Neo4jClientMock{}
		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := repo.CreateCodeRelationship(nil, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance is required but was nil").Error())
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty code", t, func() {

		code := ""

		neo4jMock := &mocks.Neo4jClientMock{}
		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := repo.CreateCodeRelationship(instance, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("code is required but was empty").Error())
			})

			Convey("And Neo4j.ExecStmt is never called", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a Neo4j.ExecStmt returns an error", t, func() {

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}

		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := repo.CreateCodeRelationship(instance, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.ExecStmt returned an error").Error())
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createInstanceToCodeRelStmt, "_"+instanceID+"_Instance")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldResemble, map[string]interface{}{
					"code": code,
				})
			})
		})
	})

	Convey("Given that result.RowsAffected returns an error", t, func() {

		resultMock := &mocks.NeoResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return -1, errorMock
			},
		}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return resultMock, nil
			},
		}

		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := repo.CreateCodeRelationship(instance, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "result.RowsAffected() returned an error").Error())
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createInstanceToCodeRelStmt, "_"+instanceID+"_Instance")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldResemble, map[string]interface{}{
					"code": code,
				})
			})
		})
	})

	Convey("Given that result.RowsAffected is not 1", t, func() {

		resultMock := &mocks.NeoResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 0, nil
			},
		}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return resultMock, nil
			},
		}

		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := repo.CreateCodeRelationship(instance, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "failed to find the code node to link to the instance node")
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createInstanceToCodeRelStmt, "_"+instanceID+"_Instance")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldResemble, map[string]interface{}{
					"code": code,
				})
			})
		})
	})

	Convey("Given a Neo4j.ExecStmt returns no error", t, func() {

		resultMock := &mocks.NeoResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 1, nil
			},
		}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return resultMock, nil
			},
		}

		connMock := &mocks.NeoConnMock{}

		repo := InstanceRepository{
			neo4j: neo4jMock,
			conn:  connMock,
		}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := repo.CreateCodeRelationship(instance, code)

			Convey("Then no error is returned", func() {
				So(err, ShouldResemble, nil)
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(createInstanceToCodeRelStmt, "_"+instanceID+"_Instance")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldResemble, map[string]interface{}{
					"code": code,
				})
			})
		})
	})
}

func TestInstanceRepository_Exists(t *testing.T) {
	Convey("Given the repository has been configured correctly", t, func() {
		var count int64 = 1
		data := [][]interface{}{[]interface{}{count}}
		rows := &common.NeoRows{Data: data}

		neoMock := &mocks.Neo4jClientMock{
			QueryFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
				return rows, nil
			},
		}
		connMock := &mocks.NeoConnMock{}
		repo := InstanceRepository{neo4j: neoMock, conn: connMock}

		Convey("When Exists is invoked for an existing instance", func() {
			exists, err := repo.Exists(instance)

			Convey("Then reposity returns the expected result", func() {
				So(exists, ShouldBeTrue)
				So(err, ShouldBeNil)
			})

			Convey("And neo4j.Query is called 1 time with the expected parameters", func() {
				So(len(neoMock.QueryCalls()), ShouldEqual, 1)

				countStmt := fmt.Sprintf(countInstanceStmt, fmt.Sprintf(instanceLabelFmt, instance.GetID()))
				So(neoMock.QueryCalls()[0].Query, ShouldEqual, countStmt)
				So(neoMock.QueryCalls()[0].Params, ShouldBeNil)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neoMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})

		Convey("When neo4j.Query returns an error", func() {
			neoMock.QueryFunc = func(conn bolt.Conn, query string, params map[string]interface{}) (*common.NeoRows, error) {
				return nil, errorMock
			}

			exists, err := repo.Exists(instance)

			Convey("Then the error is propegated back to the caller", func() {
				So(exists, ShouldBeFalse)
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.Query returned an error").Error())
			})

			Convey("And neo4j.Query is called 1 time with the expected parameters", func() {
				So(len(neoMock.QueryCalls()), ShouldEqual, 1)

				countStmt := fmt.Sprintf(countInstanceStmt, fmt.Sprintf(instanceLabelFmt, instance.GetID()))
				So(neoMock.QueryCalls()[0].Query, ShouldEqual, countStmt)
				So(neoMock.QueryCalls()[0].Params, ShouldBeNil)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neoMock.ExecStmtCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestNewInstanceRepository(t *testing.T) {
	Convey("Given valid parameters", t, func() {
		connMock := &mocks.NeoConnMock{}
		neo4jCliMock := &mocks.Neo4jClientMock{}
		connectionPool := &mocks.NeoDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return connMock, nil
			},
		}

		Convey("When NewInstanceRepository is called", func() {
			repo, err := NewInstanceRepository(connectionPool, neo4jCliMock)

			Convey("Then the expected repository is returned with no error", func() {
				So(repo.conn, ShouldEqual, connMock)
				So(repo.neo4j, ShouldEqual, neo4jCliMock)
				So(err, ShouldBeNil)
			})

			Convey("And connectionPool.OpenPool is called 1 time", func() {
				So(len(connectionPool.OpenPoolCalls()), ShouldEqual, 1)
			})
		})

		Convey("When connectionPool.OpenPool returns an error", func() {
			connectionPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return nil, errorMock
			}

			repo, err := NewInstanceRepository(connectionPool, neo4jCliMock)
			Convey("Then the error is propagated", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "connPool.OpenPool returned an error").Error())
				So(repo, ShouldBeNil)
			})
		})
	})
}

func TestInstanceRepository_Close(t *testing.T) {
	Convey("Given InstanceRepository.conn is not nil", t, func() {
		connMock := &mocks.NeoConnMock{
			CloseFunc: func() error {
				return nil
			},
		}
		repo := InstanceRepository{conn: connMock}

		Convey("When Close is invoked", func() {
			repo.Close()

			Convey("Then conn.Close is called 1 time", func() {
				So(len(connMock.CloseCalls()), ShouldEqual, 1)
			})
		})
	})
}
