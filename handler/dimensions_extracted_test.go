package handler

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"errors"
)

var test_instance_id = "1234567890"

var d1 = &model.Dimension{
	Dimension_ID: "1234567890_Geography",
	Value:        "England",
	NodeId:       "1",
}

var d2 = &model.Dimension{
	Dimension_ID: "1234567890_Geography",
	Value:        "Wales",
	NodeId:       "2",
}

func TestDimensionsExtractedEventHandler_HandleEvent(t *testing.T) {

	Convey("Given the handler has been configured", t, func() {
		// Set up mocks
		instanceRepositoryMock := &InstanceRepositoryMock{
			AddDimensionsFunc: func(instance *model.Instance) error {
				return nil
			},
			CreateFunc: func(instance *model.Instance) error {
				return nil
			},
		}

		dimensionRepository := &DimensionRepositoryMock{
			InsertFunc: func(instance *model.Instance, d *model.Dimension) (*model.Dimension, error) {
				return d, nil
			},
		}

		importAPIMock := &ImportAPIClientMock{
			GetDimensionsFunc: func(instanceID string) ([]*model.Dimension, error) {
				return []*model.Dimension{d1, d2}, nil
			},
			PutDimensionNodeIDFunc: func(instanceID string, d *model.Dimension) error {
				return nil
			},
		}

		handler := DimensionsExtractedEventHandler{
			CreateDimensionRepository: func() DimensionRepository {
				return dimensionRepository
			},
			InstanceRepository: instanceRepositoryMock,
			ImportAPI:          importAPIMock,
		}

		Convey("When given a valid event", func() {
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}
			handler.HandleEvent(event)

			Convey("Then ImportAPI.GetDimensions is called 1 time with the expected parameters", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
				So(importAPIMock.GetDimensionsCalls()[0].InstanceID, ShouldEqual, test_instance_id)
			})

			Convey("And DimensionRepository.Insert is called 2 times with the expected parameters", func() {
				calls := dimensionRepository.InsertCalls()
				So(len(calls), ShouldEqual, 2)

				expectedInstance := &model.Instance{InstanceID: test_instance_id, Dimensions: make([]interface{}, 0)}
				So(calls[0].Instance, ShouldResemble, expectedInstance)
				So(calls[0].Dimension, ShouldResemble, d1)

				So(calls[1].Instance, ShouldResemble, expectedInstance)
				So(calls[1].Dimension, ShouldResemble, d2)
			})

			Convey("And ImportAPI.PutDimensionNodeID is called 2 times with the expected parameters", func() {
				calls := importAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].InstanceID, ShouldEqual, test_instance_id)
				So(calls[1].InstanceID, ShouldEqual, test_instance_id)
				So(calls[0].Dimension, ShouldEqual, d1)
				So(calls[1].Dimension, ShouldEqual, d2)
			})

			Convey("And InstanceRepository.AddDimensions is called 1 time with the expected parameters", func() {
				calls := instanceRepositoryMock.AddDimensionsCalls()
				So(len(calls), ShouldEqual, 1)

				expectedInstance := &model.Instance{InstanceID: test_instance_id, Dimensions: make([]interface{}, 0)}
				So(calls[0].Instance, ShouldResemble, expectedInstance)
			})
		})

		Convey("When given an invalid event", func() {
			err := handler.HandleEvent(model.DimensionsExtractedEvent{})

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

		Convey("When ImportAPI.GetDimensions returns an error", func() {
			getDimensionsErr := errors.New("Get Dimensions error")
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}

			importAPIMock.GetDimensionsFunc = func(instanceID string) ([]*model.Dimension, error) {
				return nil, getDimensionsErr
			}

			err := handler.HandleEvent(event)

			Convey("Then the ImportAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
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
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}
			instance := &model.Instance{InstanceID: test_instance_id, Dimensions: make([]interface{}, 0)}

			instanceRepositoryMock.CreateFunc = func(instance *model.Instance) error {
				return expectedErr
			}

			err := handler.HandleEvent(event)

			Convey("Then the ImportAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("And InsatnceRepository.Create is called 1 time with the expected parameters", func() {
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
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}
			instance := &model.Instance{InstanceID: test_instance_id, Dimensions: make([]interface{}, 0)}

			dimensionRepository.InsertFunc = func(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, expectedErr
			}
			err := handler.HandleEvent(event)

			Convey("Then the ImportAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
			})

			Convey("And InsatnceRepository.Create is called 1 time with the expected parameters", func() {
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

		Convey("When ImportAPI.PutDimensionNodeID returns an error", func() {
			expectedErr := errors.New("Put Node ID error")
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}
			instance := &model.Instance{InstanceID: test_instance_id, Dimensions: make([]interface{}, 0)}

			dimensionRepository.InsertFunc = func(instance *model.Instance, dimension *model.Dimension) (*model.Dimension, error) {
				return dimension, nil
			}
			importAPIMock.PutDimensionNodeIDFunc = func(instanceID string, dimension *model.Dimension) error {
				return expectedErr
			}

			err := handler.HandleEvent(event)

			Convey("Then the ImportAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
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

			Convey("And ImportAPI.PutDimensionNodeID is called 1 time with the expected parameters", func() {
				calls := importAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 1)

				So(calls[0].InstanceID, ShouldEqual, test_instance_id)
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
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}
			instance := &model.Instance{InstanceID: test_instance_id, Dimensions: make([]interface{}, 0)}

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

			Convey("Then the ImportAPI.GetDimensions is called 1 time", func() {
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 1)
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

			Convey("And ImportAPI.PutDimensionNodeID is called 1 time with the expected parameters", func() {
				calls := importAPIMock.PutDimensionNodeIDCalls()
				So(len(calls), ShouldEqual, 2)

				So(calls[0].InstanceID, ShouldEqual, test_instance_id)
				So(calls[0].Dimension, ShouldEqual, d1)

				So(calls[1].InstanceID, ShouldEqual, test_instance_id)
				So(calls[1].Dimension, ShouldEqual, d2)
			})

			Convey("And the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(addInsanceDimsErr))
			})
		})
	})

	Convey("Given handler.ImportAPI has not been configured", t, func() {
		instanceRepositoryMock := &InstanceRepositoryMock{}
		dimensionRepository := &DimensionRepositoryMock{}

		handler := DimensionsExtractedEventHandler{
			ImportAPI:          nil,
			InstanceRepository: instanceRepositoryMock,
			CreateDimensionRepository: func() DimensionRepository {
				return dimensionRepository
			},
		}
		Convey("When HandleEvent is called", func() {
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}
			err := handler.HandleEvent(event)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(importAPINilErr))
			})

			Convey("And the event is not handled", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.AddDimensionsCalls()), ShouldEqual, 0)
				So(len(instanceRepositoryMock.CreateCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given handler.InstanceRepository has not been configured", t, func() {
		dimensionRepository := &DimensionRepositoryMock{}
		importAPIMock := &ImportAPIClientMock{}

		handler := DimensionsExtractedEventHandler{
			ImportAPI:          importAPIMock,
			InstanceRepository: nil,
			CreateDimensionRepository: func() DimensionRepository {
				return dimensionRepository
			},
		}
		Convey("When HandleEvent is called", func() {
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}
			err := handler.HandleEvent(event)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New(InstanceRepoNilErr))
			})
			Convey("And the event is not handled", func() {
				So(len(dimensionRepository.InsertCalls()), ShouldEqual, 0)
				So(len(importAPIMock.GetDimensionsCalls()), ShouldEqual, 0)
				So(len(importAPIMock.PutDimensionNodeIDCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given handler.InstanceRepository has not been configured", t, func() {
		importAPIMock := &ImportAPIClientMock{}
		instanceRepositoryMock := &InstanceRepositoryMock{}

		handler := DimensionsExtractedEventHandler{
			ImportAPI:                 importAPIMock,
			InstanceRepository:        instanceRepositoryMock,
			CreateDimensionRepository: nil,
		}
		Convey("When HandleEvent is called", func() {
			event := model.DimensionsExtractedEvent{InstanceID: test_instance_id}
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
