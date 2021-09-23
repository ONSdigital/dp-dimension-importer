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

	Convey("Given the handler has been configured", t, func() {
		// Set up mocks
		storerMock, datasetAPIMock, completedProducer, handler := setUp()

		Convey("When given a valid event", func() {
			handler.Handle(ctx, newInstance)

			Convey("Then DatasetAPICli.GetInstanceDimensions is called 1 time with the expected parameters", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()[0].InstanceID, ShouldEqual, testInstanceID)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storerMock.InsertDimension is called 2 times with the expected parameters", func() {
				validateInsertDimensionCalls(storerMock, 2, instance.DbModel().InstanceID, d1.DbModel(), d2.DbModel())
			})

			Convey("And storerMock.GetCodeOrder is called 1 time with the expected paramters", func() {
				calls := storerMock.GetCodesOrderCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].CodeListID, ShouldEqual, testCodeListID)
				So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option})
			})

			Convey("And DatasetAPICli.PatchDimensionOption is called 1 time with the expected parameters", func() {
				calls := datasetAPIMock.PatchInstanceDimensionsCalls()
				So(len(calls), ShouldEqual, 1)

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

			Convey("And storer.AddDimensions is called 1 time with the expected parameters", func() {
				calls := storerMock.AddDimensionsCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})

			Convey("And storer.CreateCodeRelationshipCalls is called once with the expected parameters", func() {
				calls := storerMock.CreateCodeRelationshipCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(calls[0].Code, ShouldResemble, d1.DbModel().Option)

				So(calls[1].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(calls[1].Code, ShouldResemble, d2.DbModel().Option)
			})

			Convey("And storer is called once to create constraints", func() {
				So(len(storerMock.CreateInstanceConstraintCalls()), ShouldEqual, 1)
			})

			Convey("And Producer.Complete is called 1 time with the expected parameters", func() {
				calls := completedProducer.CompletedCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].E, ShouldResemble, instanceCompleted)
			})
		})

		Convey("When given an invalid event", func() {
			err := handler.Handle(ctx, event.NewInstance{})

			Convey("Then the appropriate error is returned", func() {
				So(err.Error(), ShouldResemble, "validation error instance_id required but was empty")
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(storerMock.GetCodesOrderCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PatchInstanceDimensionsCalls()), ShouldEqual, 0)
				So(len(storerMock.CreateInstanceCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When DatasetAPICli.GetInstanceDimensions returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			datasetAPIMock.GetInstanceDimensionsInBatchesFunc = func(ctx context.Context, serviceAuthToken string, instanceID string, maxWorkers, batchSize int) (dataset.Dimensions, string, error) {
				return dataset.Dimensions{}, "", errorMock
			}

			err := handler.Handle(ctx, event)

			Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, fmt.Errorf("DatasetAPICli.GetDimensions returned an error: %w", errorMock).Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(storerMock.GetCodesOrderCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PatchInstanceDimensionsCalls()), ShouldEqual, 0)
				So(len(storerMock.CreateInstanceCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When storer.CreateInstance returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.CreateInstanceFunc = func(ctx context.Context, instanceID string, csvHeaders []string) error {
				return errorMock
			}

			err := handler.Handle(ctx, event)

			Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(storerMock.GetCodesOrderCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PatchInstanceDimensionsCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, fmt.Errorf("create instance returned an error: %w", errorMock).Error())
			})
		})

		Convey("When storer.CreateCodeRelationship returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.CreateCodeRelationshipFunc = func(ctx context.Context, instanceID string, codeListID string, code string) error {
				return errorMock
			}

			err := handler.Handle(ctx, event)

			Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
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
				So(len(calls), ShouldEqual, 0)
			})

			Convey("And DatasetAPICli.PatchDimensionOption is not called", func() {
				calls := datasetAPIMock.PatchInstanceDimensionsCalls()
				So(len(calls), ShouldEqual, 0)
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, fmt.Errorf("error attempting to create relationship to code: %w", errorMock).Error())
			})
		})

		Convey("When storer.InsertDimension returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
				return dimension, errorMock
			}
			err := handler.Handle(ctx, event)

			Convey("Then the DatasetAPICli.GetInstanceDimensionsCalls is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
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
				So(len(storerMock.GetCodesOrderCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PatchInstanceDimensionsCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When storer.GetCodesOrder returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
				return dimension, nil
			}
			storerMock.GetCodesOrderFunc = func(ctx context.Context, codeListID string, codes []string) (map[string]*int, error) {
				return nil, errorMock
			}

			err := handler.Handle(ctx, event)

			Convey("Then the DatasetAPICli.GetInstanceDimensionsCalls is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})

			Convey("And storer.InsertDimension is called at least once with the expected parameters", func() {
				calls := storerMock.InsertDimensionCalls()
				So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
			})

			Convey("And storerMock.GetCodesOrder is called at least once with the expected paramters", func() {
				calls := storerMock.GetCodesOrderCalls()
				So(len(calls), ShouldBeGreaterThan, 0)

				So(calls[0].CodeListID, ShouldEqual, testCodeListID)
				So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option})
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, fmt.Errorf("error while attempting to get dimension order using codes: %w", errorMock).Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(datasetAPIMock.PatchInstanceDimensionsCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When DatasetAPICli.PatchDimensionOption returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
				return dimension, nil
			}
			datasetAPIMock.PatchInstanceDimensionsFunc = func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
				return "", errorMock
			}

			err := handler.Handle(ctx, event)

			Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})

			Convey("And storer.InsertDimension is called 2 time with the expected parameters", func() {
				calls := storerMock.InsertDimensionCalls()
				So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
			})

			Convey("And storerMock.GetCodeOrder is called 2 times with the expected paramters", func() {
				calls := storerMock.GetCodesOrderCalls()
				So(len(calls), ShouldBeGreaterThan, 0)

				So(calls[0].CodeListID, ShouldEqual, testCodeListID)
				So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option})
			})

			Convey("And DatasetAPICli.PatchDimensionOption is called at least once with the expected parameters for the first batch", func() {
				calls := datasetAPIMock.PatchInstanceDimensionsCalls()
				So(len(calls), ShouldBeGreaterThan, 0)

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
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When storer.AddDimensions returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
				return dimension, nil
			}
			datasetAPIMock.PatchInstanceDimensionsFunc = func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
				return "", nil
			}
			storerMock.AddDimensionsFunc = func(ctx context.Context, instanceID string, dimensions []interface{}) error {
				return errorMock
			}

			err := handler.Handle(ctx, event)

			Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})

			Convey("And storer.InsertDimension is called 2 time with the expected parameters", func() {
				validateInsertDimensionCalls(storerMock, 2, instance.DbModel().InstanceID, d1.DbModel(), d2.DbModel())
			})

			Convey("And storerMock.GetCodeOrder is called 1 time with the expected paramters", func() {
				calls := storerMock.GetCodesOrderCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].CodeListID, ShouldEqual, testCodeListID)
				So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option})
			})

			Convey("And DatasetAPICli.PatchDimensionOption is called 1 time with the expected parameters", func() {
				calls := datasetAPIMock.PatchInstanceDimensionsCalls()
				So(len(calls), ShouldEqual, 1)

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
				So(err.Error(), ShouldEqual, fmt.Errorf("AddDimensions returned an error: %w", errorMock).Error())
			})
		})
	})

	Convey("Given handler.DatasetAPICli has not been configured", t, func() {
		db := &storertest.StorerMock{}

		handler := handler.InstanceEventHandler{
			DatasetAPICli: nil,
			Store:         db,
		}

		Convey("When Handle is called", func() {
			event := event.NewInstance{InstanceID: testInstanceID}
			err := handler.Handle(ctx, event)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "validation error dataset api client required but was nil")
			})

			Convey("And the event is not handled", func() {
				So(len(db.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(db.AddDimensionsCalls()), ShouldEqual, 0)
				So(len(db.CreateInstanceCalls()), ShouldEqual, 0)
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
			Convey("And the event is not handled", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PatchInstanceDimensionsCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestInstanceEventHandler_Handle_ExistingInstance(t *testing.T) {
	Convey("Given an instance with the event ID already exists", t, func() {
		storerMock, datasetAPIMock, completedProducer, handler := setUp()

		// override default
		storerMock.InstanceExistsFunc = func(ctx context.Context, instanceID string) (bool, error) {
			return true, nil
		}

		Convey("When Handle is given a NewInstance event with the same instanceID", func() {
			handler.Handle(ctx, newInstance)

			Convey("Then storer.InstanceExists is called 1 time with expected parameters ", func() {
				calls := storerMock.InstanceExistsCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})

			Convey("And storer.CreateInstance is not called", func() {
				So(len(storerMock.CreateInstanceCalls()), ShouldEqual, 0)
			})

			Convey("And storer.InsertDimension is not called", func() {
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
			})

			Convey("And DatasetAPICli.PatchDimensionOption not called", func() {
				So(len(datasetAPIMock.PatchInstanceDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("and storer.AddDimensions is not called", func() {
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And storer is not called to create constraints", func() {
				So(len(storerMock.CreateInstanceConstraintCalls()), ShouldEqual, 0)
			})

			Convey("And Producer.Completed is not called", func() {
				So(len(completedProducer.CompletedCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestInstanceEventHandler_Handle_InstanceExistsErr(t *testing.T) {
	Convey("Given handler has been configured correctly", t, func() {
		storerMock, datasetAPIMock, completedProducer, handler := setUp()

		Convey("When storer.InstanceExists returns an error", func() {
			storerMock.InstanceExistsFunc = func(ctx context.Context, instanceID string) (bool, error) {
				return false, errorMock
			}

			err := handler.Handle(ctx, newInstance)

			Convey("Then handler returns the expected error", func() {
				So(err.Error(), ShouldEqual, fmt.Errorf("instance exists check returned an error: %w", errorMock).Error())
			})

			Convey("And datasetAPICli make the expected calls with the expected parameters", func() {
				So(len(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(len(datasetAPIMock.PatchInstanceDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And storer makes the expected called with the expected parameters", func() {
				So(len(storerMock.InstanceExistsCalls()), ShouldEqual, 1)
				So(storerMock.InstanceExistsCalls()[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
				So(len(storerMock.CreateInstanceCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
				So(len(storerMock.GetCodesOrderCalls()), ShouldEqual, 0)
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(storerMock.CreateInstanceConstraintCalls()), ShouldEqual, 0)
			})

			Convey("And producer is never called", func() {
				So(len(completedProducer.CompletedCalls()), ShouldEqual, 0)
			})
		})

	})
}

// Default set up for the mocks.
func setUp() (*storertest.StorerMock, *mocks.IClientMock, *mocks.CompletedProducerMock, handler.InstanceEventHandler) {
	storeMock := &storertest.StorerMock{
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

	datasetAPIMock := &mocks.IClientMock{
		GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, maxWorkers, batchSize int) (dataset.Dimensions, string, error) {
			return dataset.Dimensions{Items: []dataset.Dimension{d1Api, d2Api}}, "", nil
		},
		PatchInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
			return "", nil
		},
		GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
			return instanceApi, "", nil
		},
	}
	datasetAPIClient := &client.DatasetAPI{
		AuthToken:      "token",
		DatasetAPIHost: "host",
		Client:         datasetAPIMock,
	}

	completedProducer := &mocks.CompletedProducerMock{
		CompletedFunc: func(ctx context.Context, e event.InstanceCompleted) error {
			return nil
		},
	}

	handler := handler.InstanceEventHandler{
		Store:             storeMock,
		DatasetAPICli:     datasetAPIClient,
		Producer:          completedProducer,
		EnablePatchNodeID: true,
		BatchSize:         testBatchSize,
	}
	return storeMock, datasetAPIMock, completedProducer, handler
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
