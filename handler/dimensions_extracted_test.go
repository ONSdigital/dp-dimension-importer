package handler

import (
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"github.com/ONSdigital/dp-dimension-importer/model"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var testInstanceID = "1234567890"

var d1 = &model.Dimension{
	DimensionID: "1234567890_Geography",
	Option:      "England",
	NodeID:      "1",
}

var d2 = &model.Dimension{
	DimensionID: "1234567890_Geography",
	Option:      "Wales",
	NodeID:      "2",
}

var instance = &model.Instance{
	InstanceID: testInstanceID,
	CSVHeader:  []string{"the", "CSV", "header"},
}

func TestDimensionsExtractedEventHandler_HandleEvent(t *testing.T) {

	Convey("Given the handler has been configured", t, func() {
		// Set up mocks
		instanceRepositoryMock := &mocks.InstanceRepositoryMock{
			AddDimensionsFunc: func(instance *model.Instance) error {
				return nil
			},
			CreateFunc: func(instance *model.Instance) error {
				return nil
			},
		}

		dimensionRepository := &mocks.DimensionRepositoryMock{
			InsertFunc: func(instance *model.Instance, d *model.Dimension) (*model.Dimension, error) {
				return d, nil
			},
		}

		importAPIMock := &mocks.DatasetAPIClientMock{
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

		handler := DimensionsExtractedEventHandler{
			NewDimensionInserter: func() DimensionRepository {
				return dimensionRepository
			},
			InstanceRepository: instanceRepositoryMock,
			DatasetAPI:         importAPIMock,
		}

		Convey("When given a valid event", func() {
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}
			handler.HandleEvent(event)

			Convey("Then DatasetAPI.GetDimensions is called 1 time with the expected parameters", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
				So(importAPIMock.GetDimensionsCalls()[0].InstanceID, ShouldEqual, testInstanceID)
			})

			Convey("Then DatasetAPI.GetInstance is called 1 time", func() {
				So(len(importAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And DimensionRepository.Insert is called 2 times with the expected parameters", func() {
				calls := dimensionRepository.InsertCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].Instance, ShouldResemble, instance)
				So(calls[0].Dimension, ShouldResemble, d1)

				So(calls[1].Instance, ShouldResemble, instance)
				So(calls[1].Dimension, ShouldResemble, d2)
			})

			Convey("And DatasetAPI.PutDimensionNodeID is called 2 times with the expected parameters", func() {
				calls := importAPIMock.PutDimensionNodeIDCalls()
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
		})

		Convey("When given an invalid event", func() {
			err := handler.HandleEvent(event.DimensionsExtractedEvent{})

			Convey("Then the appropriate error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceIDNilErr))
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(importAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.CreateCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When DatasetAPI.GetDimensions returns an error", func() {
			getDimensionsErr := errors.New("Get Dimensions error")
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}

			importAPIMock.GetDimensionsFunc = func(instanceID string) ([]*model.Dimension, error) {
				return nil, getDimensionsErr
			}

			err := handler.HandleEvent(event)

			Convey("Then the DatasetAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPI.GetInstance is called 1 time", func() {
				So(len(importAPIMock.GetInstanceCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(dimensionCliErrMsg))
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(importAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.CreateCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When InstanceRepository.Create returns an error", func() {
			expectedErr := errors.New("Create Error")
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}

			instanceRepositoryMock.CreateFunc = func(instance *model.Instance) error {
				return expectedErr
			}

			err := handler.HandleEvent(event)

			Convey("Then the DatasetAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPI.GetInstance is called 1 time", func() {
				So(len(importAPIMock.GetInstanceCalls()), ShouldEqual, 1)
			})

			Convey("And InstanceRepository.Create is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.CreateCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].Instance, ShouldResemble, instance)
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(importAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})

			Convey("And the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(createInstanceErr))
			})
		})

		Convey("When DimensionRepository.Insert returns an error", func() {
			expectedErr := errors.New("Insert Error")
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}

			dimensionRepository.InsertFunc = func(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, expectedErr
			}
			err := handler.HandleEvent(event)

			Convey("Then the DatasetAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPI.GetInstance is called 1 time", func() {
				So(len(importAPIMock.GetInstanceCalls()), ShouldEqual, 1)
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
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(importAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When DatasetAPI.PutDimensionNodeID returns an error", func() {
			expectedErr := errors.New("Put Node ID error")
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}

			dimensionRepository.InsertFunc = func(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, nil
			}
			importAPIMock.PutDimensionNodeIDFunc = func(instanceID string, dimension *model.Dimension) error {
				return expectedErr
			}

			err := handler.HandleEvent(event)

			Convey("Then the DatasetAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPI.GetInstance is called 1 time", func() {
				So(len(importAPIMock.GetInstanceCalls()), ShouldEqual, 1)
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

			Convey("And DatasetAPI.PutDimensionNodeID is called 1 time with the expected parameters", func() {
				calls := importAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].InstanceID, ShouldEqual, testInstanceID)
				So(calls[0].Dimension, ShouldEqual, d1)
			})

			Convey("And the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(updateNodeIDErr))
			})

			Convey("And no further processing of the event takes place.", func() {
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
			})
		})

		Convey("When InstanceRepository.AddDimensions returns an error", func() {
			expectedErr := errors.New("Add dimensions error")
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}

			dimensionRepository.InsertFunc = func(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, nil
			}
			importAPIMock.PutDimensionNodeIDFunc = func(instanceID string, dimension *model.Dimension) error {
				return nil
			}
			instanceRepositoryMock.AddDimensionsFunc = func(instance *model.Instance) error {
				return expectedErr
			}

			err := handler.HandleEvent(event)

			Convey("Then the DatasetAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("Then DatasetAPI.GetInstance is called 1 time", func() {
				So(len(importAPIMock.GetInstanceCalls()), ShouldEqual, 1)
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

			Convey("And DatasetAPI.PutDimensionNodeID is called 1 time with the expected parameters", func() {
				calls := importAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].InstanceID, ShouldEqual, testInstanceID)
				So(calls[0].Dimension, ShouldEqual, d1)

				So(calls[1].InstanceID, ShouldEqual, testInstanceID)
				So(calls[1].Dimension, ShouldEqual, d2)
			})

			Convey("And the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(addInsanceDimsErr))
			})
		})
	})

	Convey("Given handler.DatasetAPI has not been configured", t, func() {
		instanceRepositoryMock := &mocks.InstanceRepositoryMock{}
		dimensionRepository := &mocks.DimensionRepositoryMock{}

		handler := DimensionsExtractedEventHandler{
			DatasetAPI:         nil,
			InstanceRepository: instanceRepositoryMock,
			NewDimensionInserter: func() DimensionRepository {
				return dimensionRepository
			},
		}
		Convey("When HandleEvent is called", func() {
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}
			err := handler.HandleEvent(event)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(datasetAPINilErr))
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
		importAPIMock := &mocks.DatasetAPIClientMock{}

		handler := DimensionsExtractedEventHandler{
			DatasetAPI:         importAPIMock,
			InstanceRepository: nil,
			NewDimensionInserter: func() DimensionRepository {
				return dimensionRepository
			},
		}
		Convey("When HandleEvent is called", func() {
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}
			err := handler.HandleEvent(event)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(instanceRepoNilErr))
			})
			Convey("And the event is not handled", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(importAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given handler.InstanceRepository has not been configured", t, func() {
		importAPIMock := &mocks.DatasetAPIClientMock{}
		instanceRepositoryMock := &mocks.InstanceRepositoryMock{}

		handler := DimensionsExtractedEventHandler{
			DatasetAPI:           importAPIMock,
			InstanceRepository:   instanceRepositoryMock,
			NewDimensionInserter: nil,
		}
		Convey("When HandleEvent is called", func() {
			event := event.DimensionsExtractedEvent{InstanceID: testInstanceID}
			err := handler.HandleEvent(event)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(createDimRepoNilErr))
			})
			Convey("And the event is not handled", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(importAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.CreateCalls()), ShouldEqual, 0)
			})
		})
	})
}