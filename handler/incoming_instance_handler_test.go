package handler

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/store/storertest"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testInstanceID = "1234567890"
	fileURL        = "/1/2/3"

	d1 = &model.Dimension{
		DimensionID: "1234567890_Geography",
		Option:      "England",
		NodeID:      "1",
	}

	d2 = &model.Dimension{
		DimensionID: "1234567890_Geography",
		Option:      "Wales",
		NodeID:      "2",
	}

	instance = &model.Instance{
		InstanceID: testInstanceID,
		CSVHeader:  []string{"the", "CSV", "header"},
	}

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
			handler.Handle(newInstance)

			Convey("Then DatasetAPICli.GetDimensions is called 1 time with the expected parameters", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetDimensionsCalls()[0].InstanceID, ShouldEqual, testInstanceID)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storerMock.InserDimension is called 2 times with the expected parameters", func() {
				calls := storerMock.InsertDimensionCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Dimension, ShouldResemble, d1)

				So(calls[1].Instance, ShouldResemble, instance)
				So(calls[1].Dimension, ShouldResemble, d2)
			})

			Convey("And DatasetAPICli.PutDimensionNodeID is called 2 times with the expected parameters", func() {
				calls := datasetAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].InstanceID, ShouldEqual, testInstanceID)
				So(calls[1].InstanceID, ShouldEqual, testInstanceID)
				So(calls[0].Dimension, ShouldEqual, d1)
				So(calls[1].Dimension, ShouldEqual, d2)
			})

			Convey("And storer.AddDimensions is called 1 time with the expected parameters", func() {
				calls := storerMock.AddDimensionsCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And storer.CreateCodeRelationshipCalls is called once with the expected parameters", func() {
				calls := storerMock.CreateCodeRelationshipCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Code, ShouldResemble, d1.Option)

				So(calls[1].Instance, ShouldResemble, instance)
				So(calls[1].Code, ShouldResemble, d2.Option)
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
			err := handler.Handle(event.NewInstance{})

			Convey("Then the appropriate error is returned", func() {
				So(err.Error(), ShouldResemble, "validation error instance_id required but was empty")
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(storerMock.CreateInstanceCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When DatasetAPICli.GetDimensions returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			datasetAPIMock.GetDimensionsFunc = func(instanceID string) ([]*model.Dimension, error) {
				return nil, errorMock
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "DatasetAPICli.GetDimensions returned an error").Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(storerMock.CreateInstanceCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When storer.CreateInstance returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.CreateInstanceFunc = func(ctx context.Context, instance *model.Instance) error {
				return errorMock
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "create instance returned an error").Error())
			})
		})

		Convey("When storer.CreateCodeRelationship returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.CreateCodeRelationshipFunc = func(ctx context.Context, i *model.Instance, codeListID, code string) error {
				return errorMock
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And storer.InsertDimension is called 2 times with the expected parameters", func() {
				calls := storerMock.InsertDimensionCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Dimension, ShouldResemble, d1)
			})

			Convey("And DatasetAPICli.PutDimensionNodeID is called 2 times with the expected parameters", func() {
				calls := datasetAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].InstanceID, ShouldEqual, testInstanceID)
				So(calls[0].Dimension, ShouldEqual, d1)
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "error attempting to create relationship to code").Error())
			})
		})

		Convey("When storer.InsertDimension returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, errorMock
			}
			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And storer.InsertDimension is called 1 time with the expected parameters", func() {
				calls := storerMock.InsertDimensionCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Dimension, ShouldResemble, d1)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "error while attempting to insert dimension").Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When DatasetAPICli.PutDimensionNodeID returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, nil
			}
			datasetAPIMock.PutDimensionNodeIDFunc = func(instanceID string, dimension *model.Dimension) error {
				return errorMock
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And storer.InsertDimension is called 2 time with the expected parameters", func() {
				calls := storerMock.InsertDimensionCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Dimension, ShouldResemble, d1)
			})

			Convey("And DatasetAPICli.PutDimensionNodeID is called 1 time with the expected parameters", func() {
				calls := datasetAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].InstanceID, ShouldEqual, testInstanceID)
				So(calls[0].Dimension, ShouldEqual, d1)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "DatasetAPICli.PutDimensionNodeID returned an error").Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When storer.AddDimensions returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			storerMock.InsertDimensionFunc = func(ctx context.Context, cache map[string]string, instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, nil
			}
			datasetAPIMock.PutDimensionNodeIDFunc = func(instanceID string, dimension *model.Dimension) error {
				return nil
			}
			storerMock.AddDimensionsFunc = func(ctx context.Context, instance *model.Instance) error {
				return errorMock
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And storer.CreateInstance is called 1 time with the expected parameters", func() {
				calls := storerMock.CreateInstanceCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And storer.InsertDimension is called 2 time with the expected parameters", func() {
				calls := storerMock.InsertDimensionCalls()
				So(len(calls), ShouldEqual, 2)
				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Dimension, ShouldResemble, d1)

				So(calls[1].Instance, ShouldResemble, instance)
				So(calls[1].Dimension, ShouldResemble, d2)
			})

			Convey("And DatasetAPICli.PutDimensionNodeID is called 1 time with the expected parameters", func() {
				calls := datasetAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].InstanceID, ShouldEqual, testInstanceID)
				So(calls[0].Dimension, ShouldEqual, d1)

				So(calls[1].InstanceID, ShouldEqual, testInstanceID)
				So(calls[1].Dimension, ShouldEqual, d2)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "AddDimensions returned an error").Error())
			})
		})
	})

	Convey("Given handler.DatasetAPICli has not been configured", t, func() {
		db := &storertest.StorerMock{}

		handler := InstanceEventHandler{
			DatasetAPICli: nil,
			Store:         db,
		}

		Convey("When Handle is called", func() {
			event := event.NewInstance{InstanceID: testInstanceID}
			err := handler.Handle(event)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("validation error dataset api client required but was nil").Error())
			})

			Convey("And the event is not handled", func() {
				So(len(db.InsertDimensionCalls()), ShouldEqual, 0)
				So(len(db.AddDimensionsCalls()), ShouldEqual, 0)
				So(len(db.CreateInstanceCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given handler.Store has not been configured", t, func() {
		datasetAPIMock := &mocks.DatasetAPIClientMock{}

		handler := InstanceEventHandler{
			DatasetAPICli: datasetAPIMock,
			Store:         nil,
		}
		Convey("When Handle is called", func() {
			event := event.NewInstance{InstanceID: testInstanceID}
			err := handler.Handle(event)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("validation error datastore required but was nil").Error())
			})
			Convey("And the event is not handled", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestInstanceEventHandler_Handle_ExistingInstance(t *testing.T) {
	Convey("Given an instance with the event ID already exists", t, func() {
		storerMock, datasetAPIMock, completedProducer, handler := setUp()

		// override default
		storerMock.InstanceExistsFunc = func(ctx context.Context, instance *model.Instance) (bool, error) {
			return true, nil
		}

		Convey("When Handle is given a NewInstance event with the same instanceID", func() {
			handler.Handle(newInstance)

			Convey("Then storer.InstanceExists is called 1 time with expected parameters ", func() {
				calls := storerMock.InstanceExistsCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And storer.CreateInstance is not called", func() {
				So(len(storerMock.CreateInstanceCalls()), ShouldEqual, 0)
			})

			Convey("And storer.InsertDimension is not called", func() {
				So(len(storerMock.InsertDimensionCalls()), ShouldEqual, 0)
			})

			Convey("And DatasetAPICli.PutDimensionNodeID not called", func() {
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
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
			storerMock.InstanceExistsFunc = func(ctx context.Context, instance *model.Instance) (bool, error) {
				return false, errorMock
			}

			err := handler.Handle(newInstance)

			Convey("Then handler returns the expected error", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "instance exists check returned an error").Error())
			})

			Convey("And datasetAPICli make the expected calls with the expected parameters", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetDimensionsCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
			})

			Convey("And storer makes the expected called with the expected parameters", func() {
				So(len(storerMock.InstanceExistsCalls()), ShouldEqual, 1)
				So(storerMock.InstanceExistsCalls()[0].Instance, ShouldEqual, instance)
				So(len(storerMock.CreateInstanceCalls()), ShouldEqual, 0)
				So(len(storerMock.AddDimensionsCalls()), ShouldEqual, 0)
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
func setUp() (*storertest.StorerMock, *mocks.DatasetAPIClientMock, *mocks.CompletedProducerMock, InstanceEventHandler) {
	storeMock := &storertest.StorerMock{
		InstanceExistsFunc: func(ctx context.Context, instance *model.Instance) (bool, error) {
			return false, nil
		},
		AddDimensionsFunc: func(ctx context.Context, instance *model.Instance) error {
			return nil
		},
		CreateInstanceFunc: func(ctx context.Context, instance *model.Instance) error {
			return nil
		},
		CreateCodeRelationshipFunc: func(ctx context.Context, i *model.Instance, codeListID, code string) error {
			return nil
		},
		InsertDimensionFunc: func(ctx context.Context, cache map[string]string, instance *model.Instance, d *model.Dimension) (*model.Dimension, error) {
			return d, nil
		},
		CreateInstanceConstraintFunc: func(ctx context.Context, instance *model.Instance) error {
			return nil
		},
		CloseFunc: func(ctx context.Context) error {
			// Do nothing.
			return nil
		},
	}

	datasetAPIMock := &mocks.DatasetAPIClientMock{
		GetDimensionsFunc: func(instanceID string) ([]*model.Dimension, error) {
			return []*model.Dimension{d1, d2}, nil
		},
		PutDimensionNodeIDFunc: func(instanceID string, d *model.Dimension) error {
			return nil
		},
		GetInstanceFunc: func(instanceID string) (*model.Instance, error) {
			return instance, nil
		},
	}

	completedProducer := &mocks.CompletedProducerMock{
		CompletedFunc: func(e event.InstanceCompleted) error {
			return nil
		},
	}

	handler := InstanceEventHandler{
		Store:         storeMock,
		DatasetAPICli: datasetAPIMock,
		Producer:      completedProducer,
	}
	return storeMock, datasetAPIMock, completedProducer, handler
}
