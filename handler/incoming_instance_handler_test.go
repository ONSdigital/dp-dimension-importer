package handler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/handler"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/store/storertest"
	"github.com/ONSdigital/dp-graph/v2/models"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testBatchSize = 2
)

var ctx = context.Background()

var (
	testInstanceID = "1234567890"
	fileURL        = "/1/2/3"
	testCodeListID = "myCodeList"

	d1Api = dataset.Dimension{
		DimensionID: "1234567890_Geography",
		Option:      "England",
		NodeID:      "1",
		Links: dataset.Links{
			CodeList: dataset.Link{
				ID: testCodeListID,
			},
		},
	}
	d1 = model.NewDimension(&d1Api)

	d2Api = dataset.Dimension{
		DimensionID: "1234567890_Geography",
		Option:      "Wales",
		NodeID:      "2",
		Links: dataset.Links{
			CodeList: dataset.Link{
				ID: testCodeListID,
			},
		},
	}
	d2 = model.NewDimension(&d2Api)

	d3Api = dataset.Dimension{
		DimensionID: "1234567890_Geography",
		Option:      "Scotland",
		NodeID:      "3",
		Links: dataset.Links{
			CodeList: dataset.Link{
				ID: testCodeListID,
			},
		},
	}
	d3 = model.NewDimension(&d3Api)

	d1Order = 0
	d2Order = 1

	instanceApi = dataset.Instance{
		Version: dataset.Version{
			ID:        testInstanceID,
			CSVHeader: []string{"the", "CSV", "header"},
		},
	}
	instance = model.NewInstance(&instanceApi)

	newInstance = event.NewInstance{
		InstanceID: testInstanceID,
		FileURL:    fileURL,
	}

	instanceCompleted = event.InstanceCompleted{
		FileURL:    fileURL,
		InstanceID: testInstanceID,
	}

	errorMock = errors.New("mock error")
)

func TestInstanceEventHandler_Handle(t *testing.T) {

	Convey("Given a successful handler", t, func() {
		// Set up mocks
		storeMock := storerMockHappy()
		datasetAPIMock := datasetAPIMockHappy()
		completedProducer := completedProducerHappy()
		handler := setUp(storeMock, datasetAPIMock, completedProducer)

		Convey("When a valid event is handled", func() {
			handler.Handle(ctx, newInstance)

			Convey("Then DatasetAPICli.GetInstanceDimensions is called 1 time with the expected parameters", func() {
				So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()[0].InstanceID, ShouldEqual, testInstanceID)
				So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()[0].BatchSize, ShouldEqual, testBatchSize)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time with the expected paramters", func() {
				So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)
			})

			Convey("And storerMock.InsertDimension is called 2 times with the expected parameters", func() {
				validateInsertDimensionCalls(storeMock, 3, instance.DbModel().InstanceID, d1.DbModel(), d2.DbModel(), d3.DbModel())
			})

			Convey("And storerMock.GetCodeOrder is called 2 times with the expected paramters", func() {
				calls := storeMock.GetCodesOrderCalls()
				So(calls, ShouldHaveLength, 2)
				So(calls[0].CodeListID, ShouldEqual, testCodeListID)
				So(calls[1].CodeListID, ShouldEqual, testCodeListID)
				So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option}) // first batch
				So(calls[1].Codes, ShouldResemble, []string{d3Api.Option})               // second batch (not full size)
			})

			Convey("And DatasetAPICli.PatchDimensionOption is called 2 time with the expected parameters", func() {
				calls := datasetAPIMock.PatchInstanceDimensionsCalls()
				So(calls, ShouldHaveLength, 2)

				So(calls[0].InstanceID, ShouldEqual, testInstanceID)
				So(calls[0].Upserts, ShouldBeNil)
				So(calls[0].Updates, ShouldResemble, []*dataset.OptionUpdate{ // first batch
					{
						Name:   d1Api.DimensionID,
						Option: d1Api.Option,
						NodeID: d1Api.NodeID,
						Order:  &d1Order,
					},
					{
						Name:   d2Api.DimensionID,
						Option: d2Api.Option,
						NodeID: d2Api.NodeID,
						Order:  &d2Order,
					},
				})

				So(calls[1].InstanceID, ShouldEqual, testInstanceID)
				So(calls[1].Upserts, ShouldBeNil)
				So(calls[1].Updates, ShouldResemble, []*dataset.OptionUpdate{ // second batch
					{
						Name:   d3Api.DimensionID,
						Option: d3Api.Option,
						NodeID: d3Api.NodeID,
					},
				})
			})

			Convey("And store.AddDimensions is called 1 time with the expected parameters", func() {
				calls := storeMock.AddDimensionsCalls()
				So(calls, ShouldHaveLength, 1)

				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(calls[0].Dimensions, ShouldResemble, instance.DbModel().Dimensions)
			})

			Convey("And store.CreateCodeRelationshipCalls is called 3 times with the expected parameters", func() {
				calls := storeMock.CreateCodeRelationshipCalls()
				So(calls, ShouldHaveLength, 3)

				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(calls[0].Code, ShouldResemble, d1.DbModel().Option)

				So(calls[1].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(calls[1].Code, ShouldResemble, d2.DbModel().Option)

				So(calls[2].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(calls[2].Code, ShouldResemble, d3.DbModel().Option)
			})

			Convey("And store is called once to create constraints for the expected instanceID", func() {
				So(storeMock.CreateInstanceConstraintCalls(), ShouldHaveLength, 1)
				So(storeMock.CreateInstanceConstraintCalls()[0].InstanceID, ShouldEqual, testInstanceID)
			})

			Convey("And Producer.Complete is called 1 time with the expected parameters", func() {
				calls := completedProducer.CompletedCalls()
				So(calls, ShouldHaveLength, 1)
				So(calls[0].E, ShouldResemble, instanceCompleted)
			})
		})
	})

	Convey("When an invalid event is handled", t, func() {
		handler := setUp(nil, nil, nil)
		err := handler.Handle(ctx, event.NewInstance{})

		Convey("Then the appropriate error is returned", func() {
			So(err.Error(), ShouldResemble, "validation error instance_id required but was empty")
		})
	})

	Convey("When DatasetAPICli.GetInstanceDimensionsInBatches returns an error", t, func() {
		event := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := &mocks.IClientMock{
			GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, maxWorkers, batchSize int) (dataset.Dimensions, string, error) {
				return dataset.Dimensions{}, "", errorMock
			},
		}
		handler := setUp(nil, datasetAPIMock, nil)
		err := handler.Handle(ctx, event)

		Convey("And the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("DatasetAPICli.GetDimensions returned an error: %w", errorMock).Error())
		})

		Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})
	})

	Convey("When storer.CreateInstance returns an error", t, func() {
		event := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := &storertest.StorerMock{
			InstanceExistsFunc: func(ctx context.Context, instanceID string) (bool, error) {
				return false, nil
			},
			CreateInstanceFunc: func(ctx context.Context, instanceID string, csvHeaders []string) error {
				return errorMock
			},
		}
		handler := setUp(storerMock, datasetAPIMock, nil)
		err := handler.Handle(ctx, event)

		Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})

		Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
			So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
		})

		Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
			calls := storerMock.CreateInstanceCalls()
			So(calls, ShouldHaveLength, 1)
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
		})

		Convey("And no further processing of the event takes place.", func() {
			So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
		})

		Convey("And the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("create instance returned an error: %w", errorMock).Error())
		})
	})

	Convey("When storer.CreateCodeRelationship returns an error", t, func() {
		event := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.CreateCodeRelationshipFunc = func(ctx context.Context, instanceID string, codeListID string, code string) error {
			return errorMock
		}
		handler := setUp(storerMock, datasetAPIMock, nil)
		err := handler.Handle(ctx, event)

		Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})

		Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
			So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
		})

		Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
			calls := storerMock.CreateInstanceCalls()
			So(calls, ShouldHaveLength, 1)
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
		})

		Convey("And storer.InsertDimension is called at least once with the expected instanceID and dimensionID", func() {
			calls := storerMock.InsertDimensionCalls()
			So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
		})

		Convey("And storerMock.GetCodeOrder is not called", func() {
			calls := storerMock.GetCodesOrderCalls()
			So(calls, ShouldHaveLength, 0)
		})

		Convey("And DatasetAPICli.PatchDimensionOption is not called", func() {
			calls := datasetAPIMock.PatchInstanceDimensionsCalls()
			So(calls, ShouldHaveLength, 0)
		})

		Convey("And no further processing of the event takes place.", func() {
			So(storerMock.AddDimensionsCalls(), ShouldHaveLength, 0)
		})

		Convey("And the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("error attempting to create relationship to code: %w", errorMock).Error())
		})
	})

	Convey("When storer.InsertDimension returns an error", t, func() {
		event := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
			return dimension, errorMock
		}
		handler := setUp(storerMock, datasetAPIMock, nil)
		err := handler.Handle(ctx, event)

		Convey("Then the DatasetAPICli.GetInstanceDimensionsCalls is called 1 time", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})

		Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
			So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
		})

		Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
			calls := storerMock.CreateInstanceCalls()
			So(calls, ShouldHaveLength, 1)
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
		})

		Convey("And storer.InsertDimension is called at least once with the expected parameters", func() {
			calls := storerMock.InsertDimensionCalls()
			So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
		})

		Convey("And the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("error while attempting to insert a dimension to the graph database: %w", errorMock).Error())
		})

		Convey("And no further processing of the event takes place.", func() {
			So(storerMock.GetCodesOrderCalls(), ShouldHaveLength, 0)
			So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
			So(storerMock.AddDimensionsCalls(), ShouldHaveLength, 0)
		})
	})

	Convey("When storer.GetCodesOrder returns an error", t, func() {
		event := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.GetCodesOrderFunc = func(ctx context.Context, codeListID string, codes []string) (map[string]*int, error) {
			return nil, errorMock
		}
		handler := setUp(storerMock, datasetAPIMock, nil)
		err := handler.Handle(ctx, event)

		Convey("Then the DatasetAPICli.GetInstanceDimensionsCalls is called 1 time", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})

		Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
			So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
		})

		Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
			calls := storerMock.CreateInstanceCalls()
			So(calls, ShouldHaveLength, 1)
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
		})

		Convey("And storer.InsertDimension is called at least once with the expected parameters", func() {
			calls := storerMock.InsertDimensionCalls()
			So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
		})

		Convey("And storerMock.GetCodesOrder is called 1 time with the expected paramters", func() {
			calls := storerMock.GetCodesOrderCalls()
			So(calls, ShouldHaveLength, 1)

			So(calls[0].CodeListID, ShouldEqual, testCodeListID)
			So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option})
		})

		Convey("And the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("error while attempting to get dimension order using codes: %w", errorMock).Error())
		})

		Convey("And no further processing of the event takes place.", func() {
			So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
			So(storerMock.AddDimensionsCalls(), ShouldHaveLength, 0)
		})
	})

	Convey("When DatasetAPICli.PatchInstanceDimensions returns an error", t, func() {
		event := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		datasetAPIMock.PatchInstanceDimensionsFunc = func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
			return "", errorMock
		}
		handler := setUp(storerMock, datasetAPIMock, nil)
		err := handler.Handle(ctx, event)

		Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})

		Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
			So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
		})

		Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
			calls := storerMock.CreateInstanceCalls()
			So(calls, ShouldHaveLength, 1)
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
		})

		Convey("And storer.InsertDimension is called at least once with the expected parameters", func() {
			calls := storerMock.InsertDimensionCalls()
			So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
		})

		Convey("And storerMock.GetCodeOrder is called 1 time with the expected paramters", func() {
			calls := storerMock.GetCodesOrderCalls()
			So(calls, ShouldHaveLength, 1)

			So(calls[0].CodeListID, ShouldEqual, testCodeListID)
			So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option})
		})

		Convey("And DatasetAPICli.PatchDimensionOption is called 1 time with the expected parameters for the first batch", func() {
			calls := datasetAPIMock.PatchInstanceDimensionsCalls()
			So(calls, ShouldHaveLength, 1)

			So(calls[0].InstanceID, ShouldEqual, testInstanceID)
			So(calls[0].Upserts, ShouldBeNil)
			So(calls[0].Updates, ShouldResemble, []*dataset.OptionUpdate{
				{
					Name:   d1Api.DimensionID,
					Option: d1Api.Option,
					NodeID: d1Api.NodeID,
					Order:  &d1Order,
				},
				{
					Name:   d2Api.DimensionID,
					Option: d2Api.Option,
					NodeID: d2Api.NodeID,
					Order:  &d2Order,
				},
			})
		})

		Convey("And the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("DatasetAPICli.PatchDimensionOption returned an error: %w", errorMock).Error())
		})

		Convey("And no further processing of the event takes place.", func() {
			So(storerMock.AddDimensionsCalls(), ShouldHaveLength, 0)
		})
	})

	Convey("When storer.AddDimensions returns an error", t, func() {
		event := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.AddDimensionsFunc = func(ctx context.Context, instanceID string, dimensions []interface{}) error {
			return errorMock
		}
		handler := setUp(storerMock, datasetAPIMock, nil)
		err := handler.Handle(ctx, event)

		Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})

		Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
			So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
		})

		Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
			calls := storerMock.CreateInstanceCalls()
			So(calls, ShouldHaveLength, 1)
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
		})

		Convey("And storer.InsertDimension is called 3 time with the expected parameters", func() {
			validateInsertDimensionCalls(storerMock, 3, instance.DbModel().InstanceID, d1.DbModel(), d2.DbModel(), d3.DbModel())
		})

		Convey("And storerMock.GetCodeOrder is called 2 times with the expected paramters", func() {
			calls := storerMock.GetCodesOrderCalls()
			So(calls, ShouldHaveLength, 2)
			So(calls[0].CodeListID, ShouldEqual, testCodeListID)
			So(calls[1].CodeListID, ShouldEqual, testCodeListID)
			So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option}) // first batch
			So(calls[1].Codes, ShouldResemble, []string{d3Api.Option})               // second batch (not full size)
		})

		Convey("And DatasetAPICli.PatchDimensionOption is called 2 times with the expected parameters", func() {
			calls := datasetAPIMock.PatchInstanceDimensionsCalls()
			So(calls, ShouldHaveLength, 2)

			So(calls[0].InstanceID, ShouldEqual, testInstanceID)
			So(calls[0].Upserts, ShouldBeNil)
			So(calls[0].Updates, ShouldResemble, []*dataset.OptionUpdate{
				{
					Name:   d1Api.DimensionID,
					Option: d1Api.Option,
					NodeID: d1Api.NodeID,
					Order:  &d1Order,
				},
				{
					Name:   d2Api.DimensionID,
					Option: d2Api.Option,
					NodeID: d2Api.NodeID,
					Order:  &d2Order,
				},
			})

			So(calls[1].InstanceID, ShouldEqual, testInstanceID)
			So(calls[1].Upserts, ShouldBeNil)
			So(calls[1].Updates, ShouldResemble, []*dataset.OptionUpdate{
				{
					Name:   d3Api.DimensionID,
					Option: d3Api.Option,
					NodeID: d3Api.NodeID,
				},
			})
		})

		Convey("And the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("AddDimensions returned an error: %w", errorMock).Error())
		})
	})

	Convey("Given handler.DatasetAPICli has not been configured", t, func() {
		db := &storertest.StorerMock{}

		handler := handler.InstanceEventHandler{
			Store: db,
		}

		Convey("When Handle is called", func() {
			event := event.NewInstance{InstanceID: testInstanceID}
			err := handler.Handle(ctx, event)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "validation error dataset api client required but was nil")
			})
		})
	})

	Convey("Given handler.Store has not been configured", t, func() {
		datasetAPIMock := &mocks.IClientMock{}
		datasetAPIClient := &client.DatasetAPI{
			AuthToken:      "token",
			DatasetAPIHost: "host",
			Client:         datasetAPIMock,
		}

		handler := handler.InstanceEventHandler{
			DatasetAPICli: datasetAPIClient,
			Store:         nil,
		}
		Convey("When Handle is called", func() {
			event := event.NewInstance{InstanceID: testInstanceID}
			err := handler.Handle(ctx, event)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "validation error datastore required but was nil")
			})
		})
	})
}

func TestInstanceEventHandler_Handle_ExistingInstance(t *testing.T) {
	Convey("Given an instance with the event ID already exists", t, func() {
		// Set up mocks, with existing instance
		storerMock := &storertest.StorerMock{
			InstanceExistsFunc: func(ctx context.Context, instanceID string) (bool, error) {
				return true, nil
			},
		}
		datasetAPIMock := datasetAPIMockHappy()
		handler := setUp(storerMock, datasetAPIMock, nil)

		Convey("When Handle is given a NewInstance event with the same instanceID", func() {
			handler.Handle(ctx, newInstance)

			Convey("Then storer.InstanceExists is called 1 time with expected parameters ", func() {
				calls := storerMock.InstanceExistsCalls()
				So(calls, ShouldHaveLength, 1)
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})

			Convey("And DatasetAPICli.PatchDimensionOption not called", func() {
				So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestInstanceEventHandler_Handle_InstanceExistsErr(t *testing.T) {
	Convey("Given handler has been configured correctly", t, func() {
		// Set up mocks, with InstanceExists returning an error
		storerMock := &storertest.StorerMock{
			InstanceExistsFunc: func(ctx context.Context, instanceID string) (bool, error) {
				return false, errorMock
			},
		}
		datasetAPIMock := datasetAPIMockHappy()
		handler := setUp(storerMock, datasetAPIMock, nil)

		Convey("When storer.InstanceExists returns an error", func() {
			storerMock.InstanceExistsFunc = func(ctx context.Context, instanceID string) (bool, error) {
				return false, errorMock
			}

			err := handler.Handle(ctx, newInstance)

			Convey("Then handler returns the expected error", func() {
				So(err.Error(), ShouldEqual, fmt.Errorf("instance exists check returned an error: %w", errorMock).Error())
			})

			Convey("And datasetAPICli make the expected calls with the expected parameters", func() {
				So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
			})

			Convey("And storer makes the expected called with the expected parameters", func() {
				So(storerMock.InstanceExistsCalls(), ShouldHaveLength, 1)
				So(storerMock.InstanceExistsCalls()[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})
		})
	})
}

func storerMockHappy() *storertest.StorerMock {
	return &storertest.StorerMock{
		InstanceExistsFunc: func(ctx context.Context, instanceID string) (bool, error) {
			return false, nil
		},
		AddDimensionsFunc: func(ctx context.Context, instanceID string, dimensions []interface{}) error {
			return nil
		},
		CreateInstanceFunc: func(ctx context.Context, instanceID string, csvHeaders []string) error {
			return nil
		},
		CreateCodeRelationshipFunc: func(ctx context.Context, instanceID string, codeListID string, code string) error {
			return nil
		},
		InsertDimensionFunc: func(ctx context.Context, cache map[string]string, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
			return dimension, nil
		},
		CreateInstanceConstraintFunc: func(ctx context.Context, instanceID string) error {
			return nil
		},
		GetCodesOrderFunc: func(ctx context.Context, codeListID string, codes []string) (map[string]*int, error) {
			return map[string]*int{
				"England": &d1Order,
				"Wales":   &d2Order,
			}, nil
		},
		CloseFunc: func(ctx context.Context) error {
			// Do nothing.
			return nil
		},
	}
}

func datasetAPIMockHappy() *mocks.IClientMock {
	return &mocks.IClientMock{
		GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, maxWorkers, batchSize int) (dataset.Dimensions, string, error) {
			return dataset.Dimensions{Items: []dataset.Dimension{d1Api, d2Api, d3Api}}, "", nil
		},
		PatchInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
			return "", nil
		},
		GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
			return instanceApi, "", nil
		},
	}
}
func completedProducerHappy() *mocks.CompletedProducerMock {
	return &mocks.CompletedProducerMock{
		CompletedFunc: func(ctx context.Context, e event.InstanceCompleted) error {
			return nil
		},
	}
}

// Default set up for the handler with provided mocks
func setUp(storeMock *storertest.StorerMock, datasetAPIMock *mocks.IClientMock, completedProducer *mocks.CompletedProducerMock) handler.InstanceEventHandler {
	datasetAPIClient := &client.DatasetAPI{
		AuthToken:      "token",
		DatasetAPIHost: "host",
		Client:         datasetAPIMock,
		BatchSize:      testBatchSize,
	}
	return handler.InstanceEventHandler{
		Store:             storeMock,
		DatasetAPICli:     datasetAPIClient,
		Producer:          completedProducer,
		EnablePatchNodeID: true,
		BatchSize:         testBatchSize,
	}
}

// validateInsertDimensionCalls validates the following for InsertDimensionCalls:
// - it was called exactly numCalls
// - all expected dimensions were inserted, and nothing else
func validateInsertDimensionCalls(storerMock *storertest.StorerMock, numCalls int, instanceID string, expectedDimensions ...*models.Dimension) {
	So(storerMock.InsertDimensionCalls(), ShouldHaveLength, numCalls)
	calledDimensions := []*models.Dimension{}
	for _, c := range storerMock.InsertDimensionCalls() {
		So(c.InstanceID, ShouldEqual, instanceID) // Validate all calls have expected instanceID
		calledDimensions = append(calledDimensions, c.Dimension)
	}
	So(calledDimensions, ShouldHaveLength, len(expectedDimensions))
	for _, expectedDimension := range expectedDimensions {
		found := false
		for _, f := range calledDimensions {
			if expectedDimension.DimensionID == f.DimensionID && expectedDimension.NodeID == f.NodeID && expectedDimension.Option == f.Option {
				found = true
				break
			}
		}
		So(found, ShouldBeTrue) // validate that dimension 'd' is found
	}
}
