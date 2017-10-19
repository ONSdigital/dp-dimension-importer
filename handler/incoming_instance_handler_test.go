package handler

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
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

	mockErr = errors.New("mock error")
)

func TestInstanceEventHandler_Handle(t *testing.T) {

	Convey("Given the handler has been configured", t, func() {
		// Set up mocks
		instanceRepositoryMock, dimensionRepository, datasetAPIMock, completedProducer, handler := setUp()

		Convey("When given a valid event", func() {
			handler.Handle(newInstance)

			Convey("Then DatasetAPICli.GetDimensions is called 1 time with the expected parameters", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetDimensionsCalls()[0].InstanceID, ShouldEqual, testInstanceID)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And DimensionRepository.Insert is called 2 times with the expected parameters", func() {
				calls := dimensionRepository.InsertCalls()
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

			Convey("And InstanceRepository.AddDimensions is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.AddDimensionsCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].Instance, ShouldResemble, instance)
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
				So(err.Error(), ShouldResemble, errors.New(" validation error instance_id required but was empty").Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.CreateCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When DatasetAPICli.GetDimensions returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			datasetAPIMock.GetDimensionsFunc = func(instanceID string) ([]*model.Dimension, error) {
				return nil, mockErr
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(mockErr, "DatasetAPICli.GetDimensions returned an error").Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.CreateCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When InstanceRepository.Create returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			instanceRepositoryMock.CreateFunc = func(instance *model.Instance) error {
				return mockErr
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And InstanceRepository.Create is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.CreateCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(mockErr, "instanceRepo.Create returned an error").Error())
			})
		})

		Convey("When DimensionRepository.Insert returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			dimensionRepository.InsertFunc = func(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, mockErr
			}
			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And InstanceRepository.Create is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.CreateCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And DimensionRepository.Insert is called 1 time with the expected parameters", func() {
				calls := dimensionRepository.InsertCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Dimension, ShouldResemble, d1)
			})

			Convey("And the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(mockErr, "error while attempting to insert dimension").Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When DatasetAPICli.PutDimensionNodeID returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			dimensionRepository.InsertFunc = func(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, nil
			}
			datasetAPIMock.PutDimensionNodeIDFunc = func(instanceID string, dimension *model.Dimension) error {
				return mockErr
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And InstanceRepository.Create is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.CreateCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And DimensionRepository.Insert is called 2 time with the expected parameters", func() {
				calls := dimensionRepository.InsertCalls()
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
				So(err.Error(), ShouldEqual, errors.Wrap(mockErr, "DatasetAPICli.PutDimensionNodeID returned an error").Error())
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When InstanceRepository.AddDimensions returns an error", func() {
			event := event.NewInstance{InstanceID: testInstanceID}

			dimensionRepository.InsertFunc = func(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, nil
			}
			datasetAPIMock.PutDimensionNodeIDFunc = func(instanceID string, dimension *model.Dimension) error {
				return nil
			}
			instanceRepositoryMock.AddDimensionsFunc = func(instance *model.Instance) error {
				return mockErr
			}

			err := handler.Handle(event)

			Convey("Then the DatasetAPICli.GetDimensions is called 1 time", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPICli.GetInstance is called 1 time", func() {
				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And InstanceRepository.Create is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.CreateCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And DimensionRepository.Insert is called 2 time with the expected parameters", func() {
				calls := dimensionRepository.InsertCalls()
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
				So(err.Error(), ShouldEqual, errors.Wrap(mockErr, "instanceRepo.AddDimensions returned an error").Error())
			})
		})
	})

	Convey("Given handler.DatasetAPICli has not been configured", t, func() {
		instanceRepositoryMock := &mocks.InstanceRepositoryMock{}
		dimensionRepository := &mocks.DimensionRepositoryMock{}

		handler := InstanceEventHandler{
			DatasetAPICli: nil,
			NewDimensionInserter: func() (DimensionRepository, error) {
				return dimensionRepository, nil
			},
			NewInstanceRepository: func() (InstanceRepository, error) {
				return instanceRepositoryMock, nil
			},
		}
		Convey("When Handle is called", func() {
			event := event.NewInstance{InstanceID: testInstanceID}
			err := handler.Handle(event)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New(" validation error dataset api client required but was nil").Error())
			})

			Convey("And the event is not handled", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.CreateCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given handler.InstanceRepository has not been configured", t, func() {
		dimensionRepository := &mocks.DimensionRepositoryMock{}
		datasetAPIMock := &mocks.DatasetAPIClientMock{}

		handler := InstanceEventHandler{
			DatasetAPICli:         datasetAPIMock,
			NewInstanceRepository: nil,
			NewDimensionInserter: func() (DimensionRepository, error) {
				return dimensionRepository, nil
			},
		}
		Convey("When Handle is called", func() {
			event := event.NewInstance{InstanceID: testInstanceID}
			err := handler.Handle(event)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New(" validation error new instance repository func required but was nil").Error())
			})
			Convey("And the event is not handled", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given handler.NewDimensionInserter has not been configured", t, func() {
		datasetAPIMock := &mocks.DatasetAPIClientMock{}
		instanceRepositoryMock := &mocks.InstanceRepositoryMock{}

		handler := InstanceEventHandler{
			DatasetAPICli: datasetAPIMock,
			NewInstanceRepository: func() (InstanceRepository, error) {
				return instanceRepositoryMock, nil
			},
			NewDimensionInserter: nil,
		}
		Convey("When Handle is called", func() {
			event := event.NewInstance{InstanceID: testInstanceID}
			err := handler.Handle(event)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New(" validation error new dimension inserter func required but was nil").Error())
			})
			Convey("And the event is not handled", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.CreateCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestInstanceEventHandler_Handle_ExistingInstance(t *testing.T) {
	Convey("Given an instance with the event ID already exists", t, func() {
		instanceRepositoryMock, dimensionRepository, datasetAPIMock, completedProducer, handler := setUp()

		// override default
		instanceRepositoryMock.ExistsFunc = func(instance *model.Instance) (bool, error) {
			return true, nil
		}

		Convey("When Handle is given a NewInstance event with the same instanceID", func() {
			handler.Handle(newInstance)

			Convey("Then InstanceRepository.Exists is called 1 time with expected parameters ", func() {
				calls := instanceRepositoryMock.ExistsCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And InstanceRepository.Delete is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.DeleteCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And InstanceRepository.Create is called 1 time with expected parameters", func() {
				calls := instanceRepositoryMock.CreateCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And imensionInserter.Insert is called 1 time with the expected parameters", func() {
				calls := dimensionRepository.InsertCalls()
				So(len(calls), ShouldEqual, 2)
				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Dimension, ShouldResemble, d1)
				So(calls[1].Instance, ShouldResemble, instance)
				So(calls[1].Dimension, ShouldResemble, d2)
			})

			Convey("And DatasetAPICli.PutDimensionNodeID is called 2 times with the expected parameters", func() {
				calls := datasetAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 2)
				So(calls[0].InstanceID, ShouldEqual, instance.InstanceID)
				So(calls[0].Dimension, ShouldEqual, d1)
				So(calls[1].InstanceID, ShouldEqual, instance.InstanceID)
				So(calls[1].Dimension, ShouldEqual, d2)
			})

			Convey("and InstanceRepository.AddDimensions is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.AddDimensionsCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And Producer.Completed is called 1 time with the expected parameters", func() {
				calls := completedProducer.CompletedCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].E, ShouldResemble, instanceCompleted)
			})
		})
	})
}

func TestInstanceEventHandler_Handle_InstanceExistsErr(t *testing.T) {
	Convey("Given handler has been configured correctly", t, func() {
		instanceRepoMock, dimensionRepoMock, datasetAPIMock, completedProducer, handler := setUp()

		Convey("When instanceRepository.Exists returns an error", func() {
			instanceRepoMock.ExistsFunc = func(instance *model.Instance) (bool, error) {
				return false, mockErr
			}

			err := handler.Handle(newInstance)

			Convey("Then handler returns the expected error", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(mockErr, "instance repository exists check returned an error").Error())
			})

			Convey("And datasetAPICli make the expected calls with the expected parameters", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetDimensionsCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
			})

			Convey("And instanceRepository makes the expected called with the expected parameters", func() {
				So(len(instanceRepoMock.ExistsCalls()), ShouldEqual, 1)
				So(instanceRepoMock.ExistsCalls()[0].Instance, ShouldEqual, instance)

				So(len(instanceRepoMock.DeleteCalls()), ShouldEqual, 0)
				So(len(instanceRepoMock.CreateCalls()), ShouldEqual, 0)
				So(len(instanceRepoMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And dimensionRepository is never called", func() {
				So(len(dimensionRepoMock.InsertCalls()), ShouldEqual, 0)
			})

			Convey("And producer is never called", func() {
				So(len(completedProducer.CompletedCalls()), ShouldEqual, 0)
			})
		})

	})
}

func TestInstanceEventHandler_Handle_DeleteInstanceErr(t *testing.T) {
	Convey("Given handler has been configured correctly", t, func() {
		instanceRepoMock, dimensionRepoMock, datasetAPIMock, completedProducer, handler := setUp()
		instanceRepoMock.ExistsFunc = func(instance *model.Instance) (bool, error) {
			return true, nil
		}
		instanceRepoMock.DeleteFunc = func(instance *model.Instance) error {
			return mockErr
		}

		Convey("When instanceRepository.Delete returns an error", func() {
			err := handler.Handle(newInstance)

			Convey("Then handler returns the expected error", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(mockErr, "instanceRepo.Delete returned an error").Error())
			})

			Convey("And datasetAPICli make the expected calls with the expected parameters", func() {
				So(len(datasetAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetDimensionsCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(len(datasetAPIMock.GetInstanceCalls()), ShouldEqual, 1)
				So(datasetAPIMock.GetInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)

				So(len(datasetAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
			})

			Convey("And instanceRepository makes the expected called with the expected parameters", func() {
				So(len(instanceRepoMock.ExistsCalls()), ShouldEqual, 1)
				So(instanceRepoMock.ExistsCalls()[0].Instance, ShouldEqual, instance)

				So(len(instanceRepoMock.DeleteCalls()), ShouldEqual, 1)
				So(instanceRepoMock.DeleteCalls()[0].Instance, ShouldEqual, instance)

				So(len(instanceRepoMock.CreateCalls()), ShouldEqual, 0)
				So(len(instanceRepoMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And dimensionRepository is never called", func() {
				So(len(dimensionRepoMock.InsertCalls()), ShouldEqual, 0)
			})

			Convey("And producer is never called", func() {
				So(len(completedProducer.CompletedCalls()), ShouldEqual, 0)
			})
		})
	})
}

// Default set up for the mocks.
func setUp() (*mocks.InstanceRepositoryMock, *mocks.DimensionRepositoryMock, *mocks.DatasetAPIClientMock, *mocks.CompletedProducerMock, InstanceEventHandler) {
	instanceRepositoryMock := &mocks.InstanceRepositoryMock{
		ExistsFunc: func(instance *model.Instance) (bool, error) {
			return false, nil
		},
		DeleteFunc: func(instance *model.Instance) error {
			return nil
		},
		AddDimensionsFunc: func(instance *model.Instance) error {
			return nil
		},
		CreateFunc: func(instance *model.Instance) error {
			return nil
		},
		CloseFunc: func() {
			// Do nothing.
		},
	}

	dimensionRepository := &mocks.DimensionRepositoryMock{
		InsertFunc: func(instance *model.Instance, d *model.Dimension) (*model.Dimension, error) {
			return d, nil
		},
		CloseFunc: func() {
			// Do nothing.
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
		NewDimensionInserter: func() (DimensionRepository, error) {
			return dimensionRepository, nil
		},
		NewInstanceRepository: func() (InstanceRepository, error) {
			return instanceRepositoryMock, nil
		},
		DatasetAPICli: datasetAPIMock,
		Producer:      completedProducer,
	}
	return instanceRepositoryMock, dimensionRepository, datasetAPIMock, completedProducer, handler
}
