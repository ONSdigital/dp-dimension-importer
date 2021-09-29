package handler_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

// validateDatastGetSuccessful checks that GetInstance and GetInstanceDimensionsInBatches are called exactly once with the expected paramters
func validateDatastGetSuccessful(datasetAPIMock *mocks.IClientMock) {
	Convey("Then DatasetAPICli.GetInstanceDimensions is called 1 time with the expected parameters", func() {
		So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()[0].InstanceID, ShouldEqual, testInstanceID)
		So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()[0].BatchSize, ShouldEqual, testBatchSize)
	})

	Convey("Then DatasetAPICli.GetInstance is called 1 time with the expected paramters", func() {
		So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
		So(datasetAPIMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)
	})
}

// validateCreateInstanceSuccessful checks that InstanceExists and CreateInstance are called exactly once with the expected paramters
func validateCreateInstanceSuccessful(storerMock *storertest.StorerMock) {
	Convey("Then storer.InstanceExists is called 1 time with the expected parameters to check that the instance did not exist already", func() {
		calls := storerMock.InstanceExistsCalls()
		So(calls, ShouldHaveLength, 1)
		So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
	})

	Convey("Then storer.CreateInstance is called 1 time with the expected parameters", func() {
		calls := storerMock.CreateInstanceCalls()
		So(calls, ShouldHaveLength, 1)
		So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
	})
}

// validateInsertDimensionSuccessful checks that InstanceInsert and CreateCodeRelationship are called 3 times with the expected paramters
func validateInsertDimensionSuccessful(t *testing.T, datasetAPIMock *mocks.IClientMock, storerMock *storertest.StorerMock) {
	Convey("Then storerMock.InsertDimension is called 2 times with the expected parameters", func() {
		validateStorerInsertDimensionCalls(storerMock, 3, instance.DbModel().InstanceID, d1.DbModel(), d2.DbModel(), d3.DbModel())
	})

	Convey("Then store.CreateCodeRelationshipCalls is called 3 times with the expected parameters", func() {
		calls := storerMock.CreateCodeRelationshipCalls()
		So(calls, ShouldHaveLength, 3)

		// Validate expected calls (in any order)
		d1Called := false
		d2Called := false
		d3Called := false
		for _, call := range calls {
			switch call.Code {
			case d1.DbModel().Option:
				So(d1Called, ShouldBeFalse) // called only once
				d1Called = true
				So(call.CodeListID, ShouldEqual, testCodeListID)
				So(call.InstanceID, ShouldEqual, testInstanceID)
			case d2.DbModel().Option:
				So(d2Called, ShouldBeFalse) // called only once
				d2Called = true
				So(call.CodeListID, ShouldEqual, testCodeListID)
				So(call.InstanceID, ShouldEqual, testInstanceID)
			case d3.DbModel().Option:
				So(d3Called, ShouldBeFalse) // called only once
				d3Called = true
				So(call.CodeListID, ShouldEqual, testCodeListID)
				So(call.InstanceID, ShouldEqual, testInstanceID)
			default:
				t.Fail()
			}
		}
	})
}

func validateSetOrderAndNodeIDsSuccessful(datasetAPIMock *mocks.IClientMock, storerMock *storertest.StorerMock) {

	Convey("Then storerMock.GetCodeOrder is called 2 times with the expected paramters", func() {
		calls := storerMock.GetCodesOrderCalls()
		So(calls, ShouldHaveLength, 2)
		So(calls[0].CodeListID, ShouldEqual, testCodeListID)
		So(calls[1].CodeListID, ShouldEqual, testCodeListID)
		So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option}) // first batch
		So(calls[1].Codes, ShouldResemble, []string{d3Api.Option})               // second batch (not full size)
	})

	Convey("Then DatasetAPICli.PatchDimensionOption is called 2 time with the expected parameters", func() {
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
}

func validateAddDimensionsSuccessful(storerMock *storertest.StorerMock) {
	Convey("Then store.AddDimensions is called 1 time with the expected parameters", func() {
		calls := storerMock.AddDimensionsCalls()
		So(calls, ShouldHaveLength, 1)
		So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
		So(calls[0].Dimensions, ShouldResemble, instance.DbModel().Dimensions)
	})
}

func validateCreateInstanceConstraintSuccessful(storerMock *storertest.StorerMock) {
	Convey("Then store is called once to create constraints for the expected instanceID", func() {
		So(storerMock.CreateInstanceConstraintCalls(), ShouldHaveLength, 1)
		So(storerMock.CreateInstanceConstraintCalls()[0].InstanceID, ShouldEqual, testInstanceID)
	})
}

func TestInstanceEventHandler_Handle(t *testing.T) {

	Convey("Given a successful handler", t, func() {
		// Set up mocks
		storerMock := storerMockHappy()
		datasetAPIMock := datasetAPIMockHappy()
		completedProducer := completedProducerHappy()
		h := setUp(storerMock, datasetAPIMock, completedProducer)

		Convey("When a valid event is handled", func() {
			err := h.Handle(ctx, newInstance)
			validateDatastGetSuccessful(datasetAPIMock)
			validateCreateInstanceSuccessful(storerMock)
			validateInsertDimensionSuccessful(t, datasetAPIMock, storerMock)
			validateSetOrderAndNodeIDsSuccessful(datasetAPIMock, storerMock)
			validateAddDimensionsSuccessful(storerMock)
			validateCreateInstanceConstraintSuccessful(storerMock)

			Convey("Then Producer.Complete is called 1 time with the expected parameters", func() {
				calls := completedProducer.CompletedCalls()
				So(calls, ShouldHaveLength, 1)
				So(calls[0].E, ShouldResemble, instanceCompleted)
			})

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})
		})
	})

	Convey("Given a handler with a datastore that fails from the third call (second batch) onwards", t, func() {
		// Set up mocks
		storerMock := storerMockHappy()
		numCall := 0
		numCallLock := sync.Mutex{}
		datasetAPIMock := datasetAPIMockHappy()
		storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, cacheMutex *sync.Mutex, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
			defer numCallLock.Unlock()
			numCallLock.Lock() // we need this lock because this method is called concurrently
			numCall++
			if numCall <= 2 {
				return dimension, nil
			}
			return dimension, errorMock
		}
		h := setUp(storerMock, datasetAPIMock, nil)

		Convey("When a valid event is handled", func() {
			err := h.Handle(ctx, newInstance)
			validateDatastGetSuccessful(datasetAPIMock)
			validateCreateInstanceSuccessful(storerMock)

			Convey("Then storerMock.InsertDimension is called three times, where the third call will fail", func() {
				validateStorerInsertDimensionCalls(storerMock, 3, instance.DbModel().InstanceID, d1.DbModel(), d2.DbModel(), d3.DbModel())
			})

			Convey("Then store.CreateCodeRelationshipCalls is called twice (for the first batch)", func() {
				calls := storerMock.CreateCodeRelationshipCalls()
				So(calls, ShouldHaveLength, 2)
			})

			Convey("Then storerMock.GetCodeOrder is called only once with the expected paramters for the first batch", func() {
				calls := storerMock.GetCodesOrderCalls()
				So(calls, ShouldHaveLength, 1)
				So(calls[0].CodeListID, ShouldEqual, testCodeListID)
				So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option}) // first batch
			})

			Convey("Then DatasetAPICli.PatchDimensionOption is called 1 time during the first batch handling", func() {
				calls := datasetAPIMock.PatchInstanceDimensionsCalls()
				So(calls, ShouldHaveLength, 1)
			})

			Convey("Then store.AddDimensions is not called", func() {
				calls := storerMock.AddDimensionsCalls()
				So(calls, ShouldHaveLength, 0)
			})

			Convey("Then CreateInstanceConstraint is not called", func() {
				So(storerMock.CreateInstanceConstraintCalls(), ShouldHaveLength, 0)
			})

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "error while attempting to insert a dimension to the graph database: mock error")
			})
		})
	})

	Convey("When an invalid event is handled", t, func() {
		h := setUp(nil, nil, nil)
		err := h.Handle(ctx, event.NewInstance{})

		Convey("Then the appropriate error is returned", func() {
			So(err.Error(), ShouldEqual, "event validation error: instance id is required but is empty")
		})
	})

	Convey("When DatasetAPICli.GetInstanceDimensionsInBatches returns an error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := &mocks.IClientMock{
			GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, maxWorkers, batchSize int) (dataset.Dimensions, string, error) {
				return dataset.Dimensions{}, "", errorMock
			},
		}
		h := setUp(nil, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("DatasetAPICli.GetDimensions returned an error: %w", errorMock).Error())
		})

		Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})
	})

	Convey("When DatasetAPICli.GetInstance returns an error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		datasetAPIMock.GetInstanceFunc = func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
			return dataset.Instance{}, "", errorMock
		}
		h := setUp(nil, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("dataset api client get instance returned an error: %w", errorMock).Error())
		})

		Convey("Then the DatasetAPICli.GetInstanceDimensions is called 1", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
		})

		Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
			So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 1)
		})
	})

	Convey("When storer.CreateInstance returns an error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := &storertest.StorerMock{
			InstanceExistsFunc: func(ctx context.Context, instanceID string) (bool, error) {
				return false, nil
			},
			CreateInstanceFunc: func(ctx context.Context, instanceID string, csvHeaders []string) error {
				return errorMock
			},
		}
		h := setUp(storerMock, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		validateDatastGetSuccessful(datasetAPIMock)

		Convey("Then storer.CreateInstance is called 1 time with the expected parameters", func() {
			calls := storerMock.CreateInstanceCalls()
			So(calls, ShouldHaveLength, 1)
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
		})

		Convey("Then no further processing of the event takes place.", func() {
			So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
		})

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("create instance returned an error: %w", errorMock).Error())
		})
	})

	Convey("When DatasetAPICli.GetInstanceDimensions returns an empty list of dimensions without error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := &mocks.IClientMock{
			GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, batchSize int, maxWorkers int) (dataset.Dimensions, string, error) {
				return dataset.Dimensions{}, "", nil
			},
		}
		h := setUp(nil, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		Convey("Then DatasetAPICli.GetInstanceDimensionsInBatchesCalls is called 1 time with the expected paramters", func() {
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls(), ShouldHaveLength, 1)
			So(datasetAPIMock.GetInstanceDimensionsInBatchesCalls()[0].InstanceID, ShouldEqual, testInstanceID)
		})

		Convey("Then the expected validation error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("dimensions validation error: %w", client.ErrDimensionsNil).Error())
		})
	})

	Convey("When DatasetAPICli.GetInstance returns an empty instance without error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := &mocks.IClientMock{
			GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, maxWorkers, batchSize int) (dataset.Dimensions, string, error) {
				return dataset.Dimensions{Items: []dataset.Dimension{d1Api, d2Api, d3Api}}, "", nil
			},
			GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
				return dataset.Instance{}, "", nil
			},
		}
		h := setUp(nil, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		validateDatastGetSuccessful(datasetAPIMock)

		Convey("Then the expected validation error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("instance validation error: %w", client.ErrInstanceIDEmpty).Error())
		})
	})

	Convey("When storer.CreateCodeRelationship returns an error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.CreateCodeRelationshipFunc = func(ctx context.Context, instanceID string, codeListID string, code string) error {
			return errorMock
		}
		h := setUp(storerMock, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		validateDatastGetSuccessful(datasetAPIMock)
		validateCreateInstanceSuccessful(storerMock)

		Convey("Then storer.InsertDimension is called at least once with the expected instanceID and dimensionID", func() {
			calls := storerMock.InsertDimensionCalls()
			So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
		})

		Convey("Then storerMock.GetCodeOrder is not called", func() {
			calls := storerMock.GetCodesOrderCalls()
			So(calls, ShouldHaveLength, 0)
		})

		Convey("Then DatasetAPICli.PatchDimensionOption is not called", func() {
			calls := datasetAPIMock.PatchInstanceDimensionsCalls()
			So(calls, ShouldHaveLength, 0)
		})

		Convey("Then no further processing of the event takes place.", func() {
			So(storerMock.AddDimensionsCalls(), ShouldHaveLength, 0)
		})

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("error attempting to create relationship to code: %w", errorMock).Error())
		})
	})

	Convey("When storer.InsertDimension returns an error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, cacheMutex *sync.Mutex, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
			return dimension, errorMock
		}
		h := setUp(storerMock, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		validateDatastGetSuccessful(datasetAPIMock)
		validateCreateInstanceSuccessful(storerMock)

		Convey("Then storer.InsertDimension is called at least once with the expected parameters", func() {
			calls := storerMock.InsertDimensionCalls()
			So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
		})

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("error while attempting to insert a dimension to the graph database: %w", errorMock).Error())
		})

		Convey("Then no further processing of the event takes place.", func() {
			So(storerMock.GetCodesOrderCalls(), ShouldHaveLength, 0)
			So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
			So(storerMock.AddDimensionsCalls(), ShouldHaveLength, 0)
		})
	})

	Convey("When SetOrderAndNodeIDs returns an error (due to storer.GetCodesOrder returning an error)", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.GetCodesOrderFunc = func(ctx context.Context, codeListID string, codes []string) (map[string]*int, error) {
			return nil, errorMock
		}
		h := setUp(storerMock, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		validateDatastGetSuccessful(datasetAPIMock)
		validateCreateInstanceSuccessful(storerMock)

		Convey("Then storer.InsertDimension is called at least once with the expected parameters", func() {
			calls := storerMock.InsertDimensionCalls()
			So(len(calls), ShouldBeGreaterThan, 0) // may be 1 or 2 depending on when go routines run
			So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			So(calls[0].Dimension.DimensionID, ShouldEqual, "1234567890_Geography")
		})

		Convey("Then storerMock.GetCodesOrder is called 1 time with the expected paramters", func() {
			calls := storerMock.GetCodesOrderCalls()
			So(calls, ShouldHaveLength, 1)

			So(calls[0].CodeListID, ShouldEqual, testCodeListID)
			So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option})
		})

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("error while attempting to get dimension order using codes: %w", errorMock).Error())
		})

		Convey("Then no further processing of the event takes place.", func() {
			So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
			So(storerMock.AddDimensionsCalls(), ShouldHaveLength, 0)
		})
	})

	Convey("When storer.AddDimensions returns an error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.AddDimensionsFunc = func(ctx context.Context, instanceID string, dimensions []interface{}) error {
			return errorMock
		}
		h := setUp(storerMock, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		validateDatastGetSuccessful(datasetAPIMock)
		validateCreateInstanceSuccessful(storerMock)
		validateInsertDimensionSuccessful(t, datasetAPIMock, storerMock)
		validateSetOrderAndNodeIDsSuccessful(datasetAPIMock, storerMock)

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("AddDimensions returned an error: %w", errorMock).Error())
		})
	})

	Convey("When storer.CreateInstanceConstraint returns an error", t, func() {
		e := event.NewInstance{InstanceID: testInstanceID}

		datasetAPIMock := datasetAPIMockHappy()
		storerMock := storerMockHappy()
		storerMock.CreateInstanceConstraintFunc = func(ctx context.Context, instanceID string) error {
			return errorMock
		}
		h := setUp(storerMock, datasetAPIMock, nil)
		err := h.Handle(ctx, e)

		validateDatastGetSuccessful(datasetAPIMock)
		validateCreateInstanceSuccessful(storerMock)
		validateInsertDimensionSuccessful(t, datasetAPIMock, storerMock)
		validateSetOrderAndNodeIDsSuccessful(datasetAPIMock, storerMock)
		validateAddDimensionsSuccessful(storerMock)
		validateCreateInstanceConstraintSuccessful(storerMock)

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldEqual, fmt.Errorf("error while attempting to add the unique observation constraint: %w", errorMock).Error())
		})
	})

	Convey("Given a successful handler with a failing kafka producer", t, func() {
		// Set up mocks
		storerMock := storerMockHappy()
		datasetAPIMock := datasetAPIMockHappy()
		completedProducer := &mocks.CompletedProducerMock{
			CompletedFunc: func(ctx context.Context, e event.InstanceCompleted) error {
				return errorMock
			},
		}
		h := setUp(storerMock, datasetAPIMock, completedProducer)

		Convey("When a valid event is handled", func() {
			err := h.Handle(ctx, newInstance)
			validateDatastGetSuccessful(datasetAPIMock)
			validateCreateInstanceSuccessful(storerMock)
			validateInsertDimensionSuccessful(t, datasetAPIMock, storerMock)
			validateSetOrderAndNodeIDsSuccessful(datasetAPIMock, storerMock)
			validateAddDimensionsSuccessful(storerMock)
			validateCreateInstanceConstraintSuccessful(storerMock)

			Convey("Then Producer.Complete is called 1 time with the expected parameters", func() {
				calls := completedProducer.CompletedCalls()
				So(calls, ShouldHaveLength, 1)
				So(calls[0].E, ShouldResemble, instanceCompleted)
			})

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, fmt.Errorf("Producer.Completed returned an error: %w", errorMock).Error())
			})
		})
	})
}

func TestValidate(t *testing.T) {
	Convey("Given handler.DatasetAPICli has not been configured", t, func() {
		h := handler.InstanceEventHandler{
			Store: &storertest.StorerMock{},
		}

		Convey("Then calling Validate returns the expected error", func() {
			e := event.NewInstance{InstanceID: testInstanceID}
			err := h.Validate(e)
			So(err.Error(), ShouldEqual, "event validation error: dataset api is required but is not provided")
		})
	})

	Convey("Given handler.Store has not been configured", t, func() {
		h := handler.InstanceEventHandler{
			DatasetAPICli: &client.DatasetAPI{
				AuthToken:      "token",
				DatasetAPIHost: "host",
				Client:         &mocks.IClientMock{},
			},
		}

		Convey("Then calling Validate returns the expected error", func() {
			e := event.NewInstance{InstanceID: testInstanceID}
			err := h.Validate(e)
			So(err.Error(), ShouldEqual, "event validation error: data store is required but is not provided")
		})
	})

	Convey("Given a fully configured handler", t, func() {
		h := handler.InstanceEventHandler{
			Store: &storertest.StorerMock{},
			DatasetAPICli: &client.DatasetAPI{
				AuthToken:      "token",
				DatasetAPIHost: "host",
				Client:         &mocks.IClientMock{},
			},
		}

		Convey("Then calling Validate with an empty instance returns the expected error", func() {
			e := event.NewInstance{}
			err := h.Validate(e)
			So(err.Error(), ShouldEqual, "event validation error: instance id is required but is empty")
		})

		Convey("Then calling Validate with a valid instance succeeds", func() {
			e := event.NewInstance{InstanceID: testInstanceID}
			err := h.Validate(e)
			So(err, ShouldBeNil)
		})
	})
}

func TestValidateInstance(t *testing.T) {
	Convey("Calling ValidateInstance with a nil instance returns the expected error", t, func() {
		err := handler.ValidateInstance(nil)
		So(err.Error(), ShouldEqual, "instance validation error: instance id is required but is empty")
	})

	Convey("Calling ValidateInstance with an empty instance returns the expected error", t, func() {
		err := handler.ValidateInstance(&model.Instance{})
		So(err.Error(), ShouldEqual, "instance validation error: instance id is required but is empty")
	})

	Convey("Calling ValidateInstance with a valid instance succeeds", t, func() {
		i := model.NewInstance(&dataset.Instance{
			Version: dataset.Version{
				ID: testInstanceID,
			},
		})
		err := handler.ValidateInstance(i)
		So(err, ShouldBeNil)
	})
}

func TestValidateDimensions(t *testing.T) {
	Convey("Calling ValidateDimensions with a nil dimensions array returns the expected error", t, func() {
		err := handler.ValidateDimensions(nil)
		So(err.Error(), ShouldEqual, "dimensions validation error: dimension array is required but is nil or empty")
	})

	Convey("Calling ValidateDimensions with an empty dimensions array returns the expected error", t, func() {
		err := handler.ValidateDimensions([]*model.Dimension{})
		So(err.Error(), ShouldEqual, "dimensions validation error: dimension array is required but is nil or empty")
	})
	Convey("Calling ValidateDimensions with a dimensions array with a nil dimension returns the expected error", t, func() {
		err := handler.ValidateDimensions([]*model.Dimension{nil})
		So(err.Error(), ShouldEqual, "dimensions validation error: dimension is required but is nil")
	})

	Convey("Calling ValidateDimensions with an invalid dimensions array returns the expected error", t, func() {
		d := model.NewDimension(&dataset.Dimension{})
		err := handler.ValidateDimensions([]*model.Dimension{d})
		So(err.Error(), ShouldEqual, "dimensions validation error: dimension.id is required but is empty")
	})

	Convey("Calling ValidateDimensions with a valid dimensions array succeeds", t, func() {
		d := model.NewDimension(&dataset.Dimension{
			DimensionID: "testDimension",
		})
		err := handler.ValidateDimensions([]*model.Dimension{d})
		So(err, ShouldBeNil)
	})
}

func TestSetOrderAndNodeIDs(t *testing.T) {
	d4Order := 8
	d4Api := dataset.Dimension{
		DimensionID: "otherDimension",
		Option:      "otherOption",
		NodeID:      "4",
		Links: dataset.Links{
			CodeList: dataset.Link{
				ID: "otherCodeList",
			},
		},
	}

	Convey("Given a datastore that contains 2 codelists with 3 and 1 codes respectively and a successful dataset api patch endpoint", t, func() {
		storerMock := &storertest.StorerMock{
			GetCodesOrderFunc: func(ctx context.Context, codeListID string, codes []string) (map[string]*int, error) {
				if codeListID == testCodeListID {
					return map[string]*int{
						"England":  &d1Order,
						"Wales":    &d2Order,
						"Scotland": nil,
					}, nil
				}
				return map[string]*int{
					"otherOption": &d4Order,
				}, nil
			},
		}
		datasetAPIMock := &mocks.IClientMock{
			PatchInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
				return "", nil
			},
		}
		h := setUp(storerMock, datasetAPIMock, nil)

		Convey("When SetOrderAndNodeIDs is called with all 4 dimensions", func() {
			dims := []*model.Dimension{
				model.NewDimension(&d1Api),
				model.NewDimension(&d2Api),
				model.NewDimension(&d3Api),
				model.NewDimension(&d4Api),
			}
			err := h.SetOrderAndNodeIDs(ctx, testInstanceID, dims)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then storerMock.GetCodeOrder is called twice: once for each codelistID", func() {
				calls := storerMock.GetCodesOrderCalls()
				So(calls, ShouldHaveLength, 2)

				// Validate expected calls (in any order)
				call1 := false
				call2 := false
				for _, call := range calls {
					switch call.CodeListID {
					case testCodeListID:
						So(call1, ShouldBeFalse)
						call1 = true
						So(call.Codes, ShouldResemble, []string{d1Api.Option, d2Api.Option, d3Api.Option})
					case "otherCodeList":
						So(call2, ShouldBeFalse)
						call2 = true
						So(call.Codes, ShouldResemble, []string{d4Api.Option})
					default:
						t.Fail()
					}
				}
			})

			Convey("Then DatasetAPICli.PatchDimensionOption is called once with the correctly mapped updates", func() {
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
					{
						Name:   d3Api.DimensionID,
						Option: d3Api.Option,
						NodeID: d3Api.NodeID,
					},
					{
						Name:   d4Api.DimensionID,
						Option: d4Api.Option,
						NodeID: d4Api.NodeID,
						Order:  &d4Order,
					},
				})
			})
		})
	})

	Convey("Given a datastore that contains a codelists with 1 code without order or nodeID and a successful dataset api patch endpoint", t, func() {
		d5Api := dataset.Dimension{
			DimensionID: "otherDimension",
			Option:      "unorderedOption",
			Links: dataset.Links{
				CodeList: dataset.Link{
					ID: testCodeListID,
				},
			},
		}

		storerMock := &storertest.StorerMock{
			GetCodesOrderFunc: func(ctx context.Context, codeListID string, codes []string) (map[string]*int, error) {
				return map[string]*int{
					"England":         &d1Order,
					"unorderedOption": nil,
				}, nil
			},
		}
		datasetAPIMock := &mocks.IClientMock{
			PatchInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
				return "", nil
			},
		}
		h := setUp(storerMock, datasetAPIMock, nil)

		Convey("When SetOrderAndNodeIDs is called with 2 dimensions", func() {
			dims := []*model.Dimension{
				model.NewDimension(&d1Api),
				model.NewDimension(&d5Api),
			}
			err := h.SetOrderAndNodeIDs(ctx, testInstanceID, dims)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then storerMock.GetCodeOrder is called once with the expected paramters", func() {
				calls := storerMock.GetCodesOrderCalls()
				So(calls, ShouldHaveLength, 1)
				So(calls[0].CodeListID, ShouldEqual, testCodeListID)
				So(calls[0].Codes, ShouldResemble, []string{d1Api.Option, d5Api.Option})
			})

			Convey("Then DatasetAPICli.PatchDimensionOption is called once with the correctly mapped updates, ignoring the dimension that does not have nodeID or Order", func() {
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
				})
			})
		})
	})

	Convey("Given a datastore that returns an error for GetCodesOrder", t, func() {
		storerMock := &storertest.StorerMock{
			GetCodesOrderFunc: func(ctx context.Context, codeListID string, codes []string) (map[string]*int, error) {
				return nil, errorMock
			},
		}
		h := setUp(storerMock, nil, nil)

		Convey("When SetOrderAndNodeIDs is called", func() {
			dims := []*model.Dimension{
				model.NewDimension(&d1Api),
			}
			err := h.SetOrderAndNodeIDs(ctx, testInstanceID, dims)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "error while attempting to get dimension order using codes: mock error")
			})
		})
	})

	Convey("Given a datastore that returns a valid order map in GetCodesOrder, and a dataset api with a failing Patch endpoint", t, func() {
		storerMock := &storertest.StorerMock{
			GetCodesOrderFunc: func(ctx context.Context, codeListID string, codes []string) (map[string]*int, error) {
				return map[string]*int{
					"England": &d1Order,
				}, nil
			},
		}
		datasetAPIMock := &mocks.IClientMock{
			PatchInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
				return "", errorMock
			},
		}
		h := setUp(storerMock, datasetAPIMock, nil)

		Convey("When SetOrderAndNodeIDs is called", func() {
			dims := []*model.Dimension{
				model.NewDimension(&d1Api),
			}
			err := h.SetOrderAndNodeIDs(ctx, testInstanceID, dims)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "DatasetAPICli.PatchDimensionOption returned an error: mock error")
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
		h := setUp(storerMock, datasetAPIMock, nil)

		Convey("When Handle is given a NewInstance event with the same instanceID", func() {
			err := h.Handle(ctx, newInstance)

			Convey("Then storer.InstanceExists is called 1 time with expected parameters ", func() {
				calls := storerMock.InstanceExistsCalls()
				So(calls, ShouldHaveLength, 1)
				So(calls[0].InstanceID, ShouldResemble, instance.DbModel().InstanceID)
			})

			Convey("Then DatasetAPICli.PatchDimensionOption not called", func() {
				So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
			})

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
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
		h := setUp(storerMock, datasetAPIMock, nil)

		Convey("When storer.InstanceExists returns an error", func() {
			storerMock.InstanceExistsFunc = func(ctx context.Context, instanceID string) (bool, error) {
				return false, errorMock
			}

			err := h.Handle(ctx, newInstance)

			Convey("Then handler returns the expected error", func() {
				So(err.Error(), ShouldEqual, fmt.Errorf("instance exists check returned an error: %w", errorMock).Error())
			})

			validateDatastGetSuccessful(datasetAPIMock)

			Convey("Then dataset API patch endpoint is not called", func() {
				So(datasetAPIMock.PatchInstanceDimensionsCalls(), ShouldHaveLength, 0)
			})

			Convey("Then storer makes the expected called with the expected parameters", func() {
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
		InsertDimensionFunc: func(ctx context.Context, cache map[string]string, cacheMutex *sync.Mutex, instanceID string, dimension *models.Dimension) (*models.Dimension, error) {
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
func setUp(storerMock *storertest.StorerMock, datasetAPIMock *mocks.IClientMock, completedProducer *mocks.CompletedProducerMock) handler.InstanceEventHandler {
	datasetAPIClient := &client.DatasetAPI{
		AuthToken:      "token",
		DatasetAPIHost: "host",
		Client:         datasetAPIMock,
		BatchSize:      testBatchSize,
	}
	return handler.InstanceEventHandler{
		Store:             storerMock,
		DatasetAPICli:     datasetAPIClient,
		Producer:          completedProducer,
		EnablePatchNodeID: true,
		BatchSize:         testBatchSize,
	}
}

// validateStorerInsertDimensionCalls validates the following for InsertDimensionCalls:
// - it was called exactly numCalls
// - all expected dimensions were inserted, and nothing else
func validateStorerInsertDimensionCalls(storerMock *storertest.StorerMock, numCalls int, instanceID string, expectedDimensions ...*models.Dimension) {
	// So(storerMock.InsertDimensionCalls(), ShouldHaveLength, numCalls)
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
