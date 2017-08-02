package client

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/logging"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"fmt"
)

var (
	dim = &model.Dimension{
		Dimension_ID: "_123456789_Geography",
		Value:        "UK",
		NodeId:       "1",
	}
)

func TestNeo4j_CreateUniqueConstraint(t *testing.T) {
	logging.Init(logging.DebugLevel)

	Convey("Given the driver pool has been correctly configured ", t, func() {
		mockConn := &NeoConnMock{
			ExecNeoFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errors.New(dimensionRequiredErr)
			},
		}
		mockedDriverPool := &NeoDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return nil, nil
			},
		}

		neoDriverPool = mockedDriverPool
		neo := Neo4j{}

		Convey("When CreateUniqueConstraint is called with an empty dim", func() {
			err := neo.CreateUniqueConstraint(nil)

			Convey("Then the appropriate error is returned", func() {
				So(err, ShouldResemble, errors.New(dimensionRequiredErr))
			})

			Convey("And no connection is opened", func() {
				So(len(mockedDriverPool.OpenPoolCalls()), ShouldEqual, 0)
			})

			Convey("And no statment is prepared", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 0)
			})

			Convey("And no statment is executed", func() {
				So(len(mockConn.ExecNeoCalls()), ShouldEqual, 0)
			})
		})

		Convey("When the driver pool returns and error", func() {
			expectedErr := errors.New("Driver pool error")

			// Return error on OpenPool
			mockedDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return nil, expectedErr
			}

			err := neo.CreateUniqueConstraint(dim)

			Convey("Then the appropriate error is returned", func() {
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And opening a connection is attempted once", func() {
				So(len(mockedDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And no statment is prepared", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 0)
			})

			Convey("And no statment is executed", func() {
				So(len(mockConn.ExecNeoCalls()), ShouldEqual, 0)
			})
		})

		Convey("When prepare neo returns an error", func() {
			expectedErr := errors.New("Prepare statment error")

			// Return error on prepare.
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return nil, expectedErr
			}
			// Close conn without error.
			mockConn.CloseFunc = func() error {
				return nil
			}
			// successfullt open a connection
			mockedDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}

			err := neo.CreateUniqueConstraint(dim)

			Convey("Then the appropriate error is returned", func() {
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And PrepareNeo is invoked once", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 1)
			})

			Convey("And expected statement was passed to PrepareNeo", func() {
				query := mockConn.PrepareNeoCalls()[0]
				expectedQuery := fmt.Sprintf(uniqueDimConstStmt, dim.GetDimensionLabel())
				So(query.Query, ShouldEqual, expectedQuery)
			})

			Convey("And the connection is closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
			})
		})

		Convey("When ExecNeo returns an error", func() {
			expectedErr := errors.New("ExecNeo error")

			// Return error on ExecNeo
			mockStmt := &NeoStmtMock{
				ExecNeoFunc: func(params map[string]interface{}) (bolt.Result, error) {
					return nil, expectedErr
				},
				CloseFunc: func() error {
					return nil
				},
			}
			// Success PrepareNeo
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return mockStmt, nil
			}
			// Close conn without error
			mockConn.CloseFunc = func() error {
				return nil
			}
			// successfullt open a connection
			mockedDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}

			neo := Neo4j{}
			err := neo.CreateUniqueConstraint(dim)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And PrepareNeo is invoked once", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 1)
			})

			Convey("And expected statement was passed to PrepareNeo", func() {
				query := mockConn.PrepareNeoCalls()[0]
				expectedQuery := fmt.Sprintf(uniqueDimConstStmt, dim.GetDimensionLabel())
				So(query.Query, ShouldEqual, expectedQuery)
			})

			Convey("And the connection is closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
			})

			Convey("And the statement is closed", func() {
				So(len(mockStmt.CloseCalls()), ShouldEqual, 1)
			})
		})

		Convey("When CreateInstance is successful", func() {
			mockStmt := &NeoStmtMock{
				// ExecNeo without error.
				ExecNeoFunc: func(params map[string]interface{}) (bolt.Result, error) {
					return nil, nil
				},
				CloseFunc: func() error {
					return nil
				},
			}
			// successfully prepare statement
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return mockStmt, nil
			}
			// close conn without error.
			mockConn.CloseFunc = func() error {
				return nil
			}
			// successfullt open a connection
			mockedDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}

			neo := Neo4j{}
			err := neo.CreateUniqueConstraint(dim)

			Convey("Then no errors are returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("And PrepareNeo and ExecNeo are invoked once", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 1)
				So(len(mockStmt.ExecNeoCalls()), ShouldEqual, 1)
			})

			Convey("And expected params were passed to PrepareNeo & ExecNeo", func() {
				query := mockConn.PrepareNeoCalls()[0]
				expectedQuery := fmt.Sprintf(uniqueDimConstStmt, dim.GetDimensionLabel())
				So(query.Query, ShouldEqual, expectedQuery)

				params := mockStmt.ExecNeoCalls()[0].Params
				var expected map[string]interface{}
				So(params, ShouldResemble, expected)
			})

			Convey("And the connection and statement are closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
				So(len(mockStmt.CloseCalls()), ShouldEqual, 1)
			})
		})
	})
}
