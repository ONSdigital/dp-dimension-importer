package repository

import (
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestNewObservationRepository(t *testing.T) {

	Convey("Given mock parameters that do not return errors", t, func() {

		connMock := &mocks.NeoConnMock{}
		neo4jCliMock := &mocks.Neo4jClientMock{}
		connectionPool := &mocks.NeoDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return connMock, nil
			},
		}

		Convey("When NewObservationRepository is called", func() {

			repo, err := NewObservationRepository(connectionPool, neo4jCliMock)

			Convey("Then the expected repository is returned with no error", func() {

				So(repo.conn, ShouldEqual, connMock)
				So(repo.neo4j, ShouldEqual, neo4jCliMock)
				So(err, ShouldBeNil)
			})

			Convey("And connectionPool.OpenPool is called 1 time", func() {

				So(len(connectionPool.OpenPoolCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestNewObservationRepository_ConnectionError(t *testing.T) {

	Convey("Given a mock connection pool that returns an error", t, func() {

		neo4jCliMock := &mocks.Neo4jClientMock{}
		connectionPool := &mocks.NeoDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return nil, errorMock
			},
		}

		Convey("When NewObservationRepository is called", func() {

			repo, err := NewObservationRepository(connectionPool, neo4jCliMock)

			Convey("Then the expected error is returned", func() {

				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "connPool.OpenPool returned an error").Error())
				So(repo, ShouldBeNil)
			})
		})
	})
}

func TestObservationRepository_CreateConstraint_NilInstance(t *testing.T) {

	Convey("Given an empty instance", t, func() {

		var instance *model.Instance

		connMock := &mocks.NeoConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		repo := ObservationRepository{conn: connMock}

		Convey("When CreateConstraint is invoked", func() {

			err := repo.CreateConstraint(instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance is required but was nil").Error())
			})
		})
	})
}

func TestObservationRepository_CreateConstraint_StatementError(t *testing.T) {

	Convey("Given mock Neo4j client that returns an error", t, func() {

		instance := &model.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

		connMock := &mocks.NeoConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}

		repo := ObservationRepository{conn: connMock, neo4j: neo4jMock}

		Convey("When CreateConstraint is invoked", func() {

			err := repo.CreateConstraint(instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.ExecStmt returned an error when creating observation constraint").Error())
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(uniqueObsConstraintStmt, "_"+instanceID+"_observation")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})
}

func TestObservationRepository_CreateConstraint(t *testing.T) {

	Convey("Given mock Neo4j client that returns no error", t, func() {

		instance := &model.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

		connMock := &mocks.NeoConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		neo4jMock := &mocks.Neo4jClientMock{
			ExecStmtFunc: func(conn bolt.Conn, query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		repo := ObservationRepository{conn: connMock, neo4j: neo4jMock}

		Convey("When CreateConstraint is invoked", func() {

			err := repo.CreateConstraint(instance)

			Convey("Then no error is returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("And Neo4j.ExecStmt is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecStmtCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(uniqueObsConstraintStmt, "_"+instanceID+"_observation")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})
}

func TestObservationRepository_CreateConstraint_NoInstanceID(t *testing.T) {

	Convey("Given an empty instance", t, func() {

		instance := &model.Instance{}

		connMock := &mocks.NeoConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		repo := ObservationRepository{conn: connMock}

		Convey("When CreateConstraint is invoked", func() {

			err := repo.CreateConstraint(instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance id is required but was empty").Error())
			})
		})
	})
}

func TestObservationRepository_Close(t *testing.T) {

	Convey("Given an ObservationRepository with a mock Neo4j connection", t, func() {

		connMock := &mocks.NeoConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		repo := ObservationRepository{conn: connMock}

		Convey("When Close is invoked", func() {
			repo.Close()

			Convey("Then conn.Close is called 1 time", func() {
				So(len(connMock.CloseCalls()), ShouldEqual, 1)
			})
		})
	})
}
