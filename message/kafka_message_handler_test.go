package message

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	mock "github.com/ONSdigital/dp-dimension-importer/message/message_test"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
)

func TestKafkaMessageHandler_Handle(t *testing.T) {
	newInstanceEvent := event.NewInstance{
		FileURL:    "/A/B/C/D",
		InstanceID: "1234567890",
	}
	avroBytes, _ := schema.NewInstanceSchema.Marshal(newInstanceEvent)

	handleInstanceFunc := func(e event.NewInstance) error {
		return nil
	}

	fixture := newFixture(avroBytes, handleInstanceFunc)

	Convey("Given KafkaMessageHandler has been correctly configured", t, func() {
		handler := KafkaMessageHandler{
			InstanceHandler: fixture.instanceHandler,
			ErrEventHandler: fixture.errorHandler,
		}

		Convey("When Handle is called with a valid message", func() {
			handler.Handle(fixture.message)
		})

		Convey("Then InstanceHandler.Handle is called 1 time with the expected parameters", func() {
			So(len(fixture.instanceHdlrCalls), ShouldEqual, 1)
			So(fixture.instanceHdlrCalls[0], ShouldResemble, newInstanceEvent)
		})

		Convey("And ErrEventHandler.Handle is never called", func() {
			So(len(fixture.errorHdlrCalls), ShouldEqual, 0)
		})

		Convey("And message.Commit is called 1 time", func() {
			So(len(fixture.message.CommitCalls()), ShouldEqual, 1)
		})
	})
}

func TestKafkaMessageHandler_Handle_InvalidKafkaMessage(t *testing.T) {
	handleInstanceFunc := func(e event.NewInstance) error {
		return nil
	}

	fix := newFixture([]byte("I am not a valid message"), handleInstanceFunc)

	Convey("Given KafkaMessageHandler has been correctly configured", t, func() {
		handler := KafkaMessageHandler{
			InstanceHandler: fix.instanceHandler,
			ErrEventHandler: fix.errorHandler,
		}

		Convey("When an invalid message is receieved", func() {
			handler.Handle(fix.message)

			Convey("Then ErrEventHandler.Handle is called 1 time with the expected parameters", func() {
				So(len(fix.errorHdlrCalls), ShouldEqual, 1)
				So(fix.errorHdlrCalls[0].InstanceID, ShouldEqual, "")
			})

			Convey("And InstanceHandler.Handle is never called", func() {
				So(len(fix.instanceHdlrCalls), ShouldEqual, 0)
			})

			Convey("And message.Commit is never called", func() {
				So(len(fix.message.CommitCalls()), ShouldEqual, 0)
			})
		})

	})
}

func TestKafkaMessageHandler_Handle_InstanceHandlerError(t *testing.T) {
	newInstanceEvent := event.NewInstance{
		FileURL:    "/A/B/C/D",
		InstanceID: "1234567890",
	}
	avroBytes, _ := schema.NewInstanceSchema.Marshal(newInstanceEvent)

	instanceHandlerErr := errors.New("Boom!")
	handleInstanceFunc := func(e event.NewInstance) error {
		return instanceHandlerErr
	}

	fix := newFixture(avroBytes, handleInstanceFunc)

	Convey("Given KafkaMessageHandler has been correctly configured", t, func() {
		handler := KafkaMessageHandler{
			InstanceHandler: fix.instanceHandler,
			ErrEventHandler: fix.errorHandler,
		}

		Convey("When Handle is called with a valid message", func() {
			handler.Handle(fix.message)
		})

		Convey("Then InstanceHandler.Handle is called 1 time with the expected parameters", func() {
			So(len(fix.instanceHdlrCalls), ShouldEqual, 1)
			So(fix.instanceHdlrCalls[0], ShouldResemble, newInstanceEvent)
		})

		Convey("And ErrEventHandler.Handle is called 1 time with the expected parameters", func() {
			So(len(fix.errorHdlrCalls), ShouldEqual, 1)
			So(fix.errorHdlrCalls[0].EventMsg, ShouldEqual, instanceHandlerErr.Error())
		})

		Convey("And message.Commit is never called", func() {
			So(len(fix.message.CommitCalls()), ShouldEqual, 0)
		})
	})
}

type fixture struct {
	instanceHdlrCalls []event.NewInstance
	errorHdlrCalls    []event.Error
	instanceHandler   mock.InstanceEventHandler
	errorHandler      mock.ErrorHandler
	message           *mock.KafkaMessageMock
}

func newFixture(messageBytes []byte, handleInstanceFunc func(e event.NewInstance) error) *fixture {
	instanceHdlrCalls := make([]event.NewInstance, 0)
	errorHdlrCalls := make([]event.Error, 0)

	fix := &fixture{
		instanceHdlrCalls: instanceHdlrCalls,
		errorHdlrCalls:    errorHdlrCalls,
		message: &mock.KafkaMessageMock{
			CommitFunc: func() {
				// DO Nothing
			},
			GetDataFunc: func() []byte {
				return messageBytes
			},
		},
	}

	instanceHandler := mock.InstanceEventHandler{
		HandleFunc: func(e event.NewInstance) error {
			fix.instanceHdlrCalls = append(fix.instanceHdlrCalls, e)
			return handleInstanceFunc(e)
		},
	}

	errorHandler := mock.ErrorHandler{
		HandleFunc: func(instanceID string, err error, data log.Data) {
			errEvent := event.Error{InstanceID: instanceID, EventType: "Error", EventMsg: err.Error()}
			fix.errorHdlrCalls = append(fix.errorHdlrCalls, errEvent)
		},
	}

	fix.instanceHandler = instanceHandler
	fix.errorHandler = errorHandler
	return fix
}
