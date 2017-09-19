package repository

import (
	"errors"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
	"github.com/ONSdigital/dp-dimension-importer/common"
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
		instance := &model.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

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

				expectedQuery := fmt.Sprintf(createInstanceStmt, "_"+instanceID+"_Instance", strings.Join(instance.CSVHeader, ","))
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})
}

func TestInstanceRepository_Delete(t *testing.T) {
	Convey("Given Instance repository has been configured correctly", t, func() {
		results := &mocks.NeoResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{},
				}
			},
		}

		neoMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return results, nil
			},
		}

		repo := InstanceRepository{Neo4j: neoMock}

		Convey("When Delete is called with a valid instance", func() {
			err := repo.Delete(instance)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And neo4j.ExexStmt is called 1 time with the expected parameters", func() {
				So(len(neoMock.ExecStmtCalls()), ShouldEqual, 1)
				expectedQuery := fmt.Sprintf(removeInstanceDimensionsAndRelationships, fmt.Sprintf(instanceLabelFmt, instance.GetID()))
				So(neoMock.ExecStmtCalls()[0].Query, ShouldEqual, expectedQuery)
				So(neoMock.ExecStmtCalls()[0].Params, ShouldBeNil)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neoMock.QueryCalls()), ShouldEqual, 0)
			})
		})

		Convey("When Delete returns an error", func() {
			neoMock.ExecStmtFunc = func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errors.New("Delete Failed")
			}

			err := repo.Delete(instance)

			Convey("Then repo returns error from Neo4j is returned", func() {
				So(err, ShouldResemble, errors.New("Delete Failed"))
			})

			Convey("And neo4j.ExexStmt is called 1 time with the expected parameters", func() {
				So(len(neoMock.ExecStmtCalls()), ShouldEqual, 1)
				expectedQuery := fmt.Sprintf(removeInstanceDimensionsAndRelationships, fmt.Sprintf(instanceLabelFmt, instance.GetID()))
				So(neoMock.ExecStmtCalls()[0].Query, ShouldEqual, expectedQuery)
				So(neoMock.ExecStmtCalls()[0].Params, ShouldBeNil)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neoMock.QueryCalls()), ShouldEqual, 0)
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
			QueryFunc: func(query string, params map[string]interface{}) (*common.NeoRows, error) {
				return rows, nil
			},
		}
		repo := InstanceRepository{Neo4j: neoMock}

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
			neoMock.QueryFunc = func(query string, params map[string]interface{}) (*common.NeoRows, error) {
				return nil, errors.New("Bork")
			}

			exists, err := repo.Exists(instance)

			Convey("Then the error is propegated back to the caller", func() {
				So(exists, ShouldBeFalse)
				So(err, ShouldResemble, errors.New("Bork"))
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
