package client

import (
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var (
	dim = &model.Dimension{
		DimensionID: "_123456789_Geography",
		Value:       "UK",
		NodeID:      "1",
	}
)

func TestNeo4j_Query(t *testing.T) {
	Convey("Given the driver pool has been correctly configured ", t, func() {
		query := "Valar morghulis"
		expectedErr := errors.New("I am expected")
		params := map[string]interface{}{"param1": "Valar dohaeris"}

		// mocks
		mockConn := &mocks.NeoConnMock{}
		mockDriverPool := &mocks.NeoDriverPoolMock{}
		mockStmt := &mocks.NeoStmtMock{}
		mockRows := &mocks.NeoQueryRowsMock{}

		neo := Neo4j{
			driverPool: mockDriverPool,
		}

		Convey("Given an empty statement", func() {
			query = ""

			Convey("When Query is called", func() {
				rows, err := neo.Query(query, nil)

				Convey("Then neoDriverPool.OpenPool is never called", func() {
					So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 0)
				})

				Convey("And the expected error response is returned", func() {
					So(err, ShouldResemble, errors.New(errQueryWasEmpty))
					So(rows, ShouldEqual, nil)
				})
			})
		})

		Convey("When Query is called with valid parameters", func() {
			rowsData := [][]interface{}{[]interface{}{1}}

			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return mockStmt, nil
			}
			mockConn.CloseFunc = closeNoErr
			mockStmt.QueryNeoFunc = func(params map[string]interface{}) (bolt.Rows, error) {
				return mockRows, nil
			}
			mockStmt.CloseFunc = closeNoErr
			mockRows.AllFunc = func() ([][]interface{}, map[string]interface{}, error) {
				return rowsData, nil, nil
			}
			mockRows.CloseFunc = closeNoErr

			expectedRows := &common.NeoRows{Data: [][]interface{}{[]interface{}{1}}}
			neoRows, err := neo.Query(query, params)

			Convey("Then the expected result should be returned and no errors", func() {
				So(err, ShouldEqual, nil)
				So(neoRows, ShouldResemble, expectedRows)
			})

			Convey("And neoDriverPool.OpenPool should be called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And conn.PrepareNeo should be called 1 time with the expected parameters", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 1)
				So(mockConn.PrepareNeoCalls()[0].Query, ShouldEqual, query)
			})

			Convey("And neoStmt.QueryNeo should be called 1 time with the expected parameters", func() {
				So(len(mockStmt.QueryNeoCalls()), ShouldEqual, 1)
				So(mockStmt.QueryNeoCalls()[0].Params, ShouldEqual, params)
			})

			Convey("And rows.All should be called 1 time", func() {
				So(len(mockRows.AllCalls()), ShouldEqual, 1)
			})

			Convey("And conn, stmt & rows should all be closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
				So(len(mockStmt.CloseCalls()), ShouldEqual, 1)
				So(len(mockRows.CloseCalls()), ShouldEqual, 1)
			})
		})

		Convey("When retrieving the all rows returns an error", func() {
			rowsData := [][]interface{}{[]interface{}{1}}

			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return mockStmt, nil
			}
			mockConn.CloseFunc = closeNoErr
			mockStmt.QueryNeoFunc = func(params map[string]interface{}) (bolt.Rows, error) {
				return mockRows, nil
			}
			mockStmt.CloseFunc = closeNoErr
			mockRows.AllFunc = func() ([][]interface{}, map[string]interface{}, error) {
				return rowsData, nil, expectedErr
			}
			mockRows.CloseFunc = closeNoErr

			// run test
			neoRows, err := neo.Query(query, params)

			Convey("Then the expected result should be returned and no errors", func() {
				So(err, ShouldResemble, errors.New(errRetrievingRows))
				So(neoRows, ShouldEqual, nil)
			})

			Convey("And neoDriverPool.OpenPool should be called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And conn.PrepareNeo should be called 1 time with the expected parameters", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 1)
				So(mockConn.PrepareNeoCalls()[0].Query, ShouldEqual, query)
			})

			Convey("And neoStmt.QueryNeo should be called 1 time with the expected parameters", func() {
				So(len(mockStmt.QueryNeoCalls()), ShouldEqual, 1)
				So(mockStmt.QueryNeoCalls()[0].Params, ShouldEqual, params)
			})

			Convey("And conn, stmt & rows should all be closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
				So(len(mockStmt.CloseCalls()), ShouldEqual, 1)
				So(len(mockRows.CloseCalls()), ShouldEqual, 1)
			})
		})

		Convey("When neoDriverPool.OpenPool returns an error", func() {
			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) { return nil, expectedErr }

			// run test
			rows, err := neo.Query(query, params)

			Convey("Then neoDriverPool.OpenPool is called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})
			Convey("And the expected error response is returned", func() {
				So(err, ShouldResemble, expectedErr)
				So(rows, ShouldEqual, nil)
			})
		})

		Convey("When neoStmt.PrepareNeo returns an error", func() {
			// Set up mocks.
			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return nil, expectedErr
			}
			mockConn.CloseFunc = closeNoErr

			// run test
			rows, err := neo.Query(query, params)

			Convey("Then neoDriverPool.OpenPool is called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And conn.PrepareNeo is called 1 time with the expected params", func() {
				calls := mockConn.PrepareNeoCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Query, ShouldEqual, query)
			})

			Convey("And the connection is closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
			})

			Convey("And the expected error response is returned", func() {
				So(err, ShouldResemble, expectedErr)
				So(rows, ShouldEqual, nil)
			})
		})

		Convey("When neoStmt.QueryNeo returns an error", func() {
			// Set up mocks.
			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return mockStmt, nil
			}
			mockStmt.QueryNeoFunc = func(params map[string]interface{}) (bolt.Rows, error) {
				return nil, expectedErr
			}
			mockConn.CloseFunc = closeNoErr
			mockStmt.CloseFunc = closeNoErr

			// Run test
			rows, err := neo.Query(query, params)

			Convey("Then neoDriverPool.OpenPool is called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And conn.PrepareNeo is called 1 time with the expected params", func() {
				calls := mockConn.PrepareNeoCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Query, ShouldEqual, query)
			})

			Convey("And neoStmt.QueryNeo is called 1 time with the expected parameters", func() {
				calls := mockStmt.QueryNeoCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Params, ShouldEqual, params)
			})

			Convey("And the statement and connection are closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
				So(len(mockStmt.CloseCalls()), ShouldEqual, 1)
			})

			Convey("And the expected error response is returned", func() {
				So(err, ShouldResemble, expectedErr)
				So(rows, ShouldEqual, nil)
			})
		})

	})
}

func TestNeo4j_ExecStmt(t *testing.T) {
	Convey("Given the driver pool has been configured correctly", t, func() {
		stmt := "Valar morghulis"
		expectedErr := errors.New("I am expected")
		params := map[string]interface{}{"param1": "Valar dohaeris"}

		// mocks
		mockConn := &mocks.NeoConnMock{}
		mockDriverPool := &mocks.NeoDriverPoolMock{}
		mockStmt := &mocks.NeoStmtMock{}
		mockResult := &mocks.NeoResultMock{}

		neo := Neo4j{
			driverPool: mockDriverPool,
		}

		Convey("When ExecStmt is called with valid parameters", func() {
			// set up mocks.
			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return mockStmt, nil
			}
			mockConn.CloseFunc = closeNoErr
			mockStmt.ExecNeoFunc = func(params map[string]interface{}) (bolt.Result, error) {
				return mockResult, nil
			}
			mockStmt.CloseFunc = closeNoErr

			// Run the test
			results, err := neo.ExecStmt(stmt, params)

			Convey("Then the expected result should be returned and no errors", func() {
				So(err, ShouldEqual, nil)
				So(results, ShouldResemble, mockResult)
			})

			Convey("And neoDriverPool.OpenPool is called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And conn.PrepareNeo should be called 1 time with the expected parameters", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 1)
				So(mockConn.PrepareNeoCalls()[0].Query, ShouldEqual, stmt)
			})

			Convey("And neoStmt.ExecNeo is called 1 time with the expected parameters", func() {
				calls := mockStmt.ExecNeoCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Params, ShouldEqual, params)
			})

			Convey("And the connection and statement are closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
				So(len(mockStmt.CloseCalls()), ShouldEqual, 1)
			})
		})

		Convey("When ExecStmt is called with an empty stmt parameter", func() {
			stmt = ""
			results, err := neo.ExecStmt(stmt, params)

			Convey("Then no results and the expected error are returned", func() {
				So(err, ShouldResemble, errors.New(errStmtWasEmpty))
				So(results, ShouldEqual, nil)
			})

			Convey("And neoDriverPool.OpenPool is never called", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 0)
			})
		})

		Convey("When neoDriverPool.OpenPool returns an error", func() {
			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) { return nil, expectedErr }

			rows, err := neo.ExecStmt(stmt, params)

			Convey("Then neoDriverPool.OpenPool is called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And the expected error response is returned", func() {
				So(err, ShouldResemble, expectedErr)
				So(rows, ShouldEqual, nil)
			})
		})

		Convey("When neoStmt.PrepareNeo returns an error", func() {
			// Set up mocks.
			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return nil, expectedErr
			}
			mockConn.CloseFunc = closeNoErr

			results, err := neo.ExecStmt(stmt, params)

			Convey("Then neoDriverPool.OpenPool is called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And conn.PrepareNeo is called 1 time with the expected params", func() {
				calls := mockConn.PrepareNeoCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Query, ShouldEqual, stmt)
			})

			Convey("And the connection is closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
			})

			Convey("And the expected error response is returned", func() {
				So(err, ShouldResemble, expectedErr)
				So(results, ShouldEqual, nil)
			})
		})

		Convey("When neoStmt.ExecNeo returns an error", func() {
			// set up mocks.
			mockDriverPool.OpenPoolFunc = func() (bolt.Conn, error) {
				return mockConn, nil
			}
			mockConn.PrepareNeoFunc = func(query string) (bolt.Stmt, error) {
				return mockStmt, nil
			}
			mockConn.CloseFunc = closeNoErr
			mockStmt.ExecNeoFunc = func(params map[string]interface{}) (bolt.Result, error) {
				return nil, expectedErr
			}
			mockStmt.CloseFunc = closeNoErr

			results, err := neo.ExecStmt(stmt, params)

			Convey("Then neoDriverPool.OpenPool is called 1 time", func() {
				So(len(mockDriverPool.OpenPoolCalls()), ShouldEqual, 1)
			})

			Convey("And conn.PrepareNeo should be called 1 time with the expected parameters", func() {
				So(len(mockConn.PrepareNeoCalls()), ShouldEqual, 1)
				So(mockConn.PrepareNeoCalls()[0].Query, ShouldEqual, stmt)
			})

			Convey("And neoStmt.ExecNeo is called 1 time with the expected parameters", func() {
				calls := mockStmt.ExecNeoCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Params, ShouldEqual, params)
			})

			Convey("And the connection and statement are closed", func() {
				So(len(mockConn.CloseCalls()), ShouldEqual, 1)
				So(len(mockStmt.CloseCalls()), ShouldEqual, 1)
			})

			Convey("And the expected error response is returned", func() {
				So(err, ShouldResemble, expectedErr)
				So(results, ShouldEqual, nil)
			})
		})
	})
}

func closeNoErr() error {
	return nil
}
