package message_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/message"
	mock "github.com/ONSdigital/dp-dimension-importer/message/mock"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/dp-kafka/kafkatest"
	"github.com/ONSdigital/dp-reporter-client/reporter/reportertest"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
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

	Convey("Given KafkaMessageReceiver has been correctly configured", t, func() {
		handler := message.KafkaMessageReceiver{
			InstanceHandler: fixture.instanceHandler,
			ErrorReporter:   fixture.errorReporter,
		}

		Convey("When OnMessage is called with a valid message", func() {
			handler.OnMessage(fixture.message)
		})

		Convey("Then InstanceHandler.OnMessage is called 1 time with the expected parameters", func() {
			So(len(fixture.instanceHdlrCalls), ShouldEqual, 1)
			So(fixture.instanceHdlrCalls[0], ShouldResemble, newInstanceEvent)
		})

		Convey("And ErrorReporter.Notify is never called", func() {
			So(len(fixture.errorReporter.NotifyCalls()), ShouldEqual, 0)
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

	Convey("Given KafkaMessageReceiver has been correctly configured", t, func() {
		handler := message.KafkaMessageReceiver{
			InstanceHandler: fix.instanceHandler,
			ErrorReporter:   fix.errorReporter,
		}

		Convey("When an invalid message is received", func() {
			handler.OnMessage(fix.message)

			Convey("Then ErrorReporter.Notify is never called", func() {
				So(len(fix.errorReporter.NotifyCalls()), ShouldEqual, 0)
			})

			Convey("And InstanceHandler.OnMessage is never called", func() {
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

	Convey("Given KafkaMessageReceiver has been correctly configured", t, func() {
		handler := message.KafkaMessageReceiver{
			InstanceHandler: fix.instanceHandler,
			ErrorReporter:   fix.errorReporter,
		}

		Convey("When OnMessage is called with a valid message", func() {
			handler.OnMessage(fix.message)
		})

		Convey("Then InstanceHandler.OnMessage is called 1 time with the expected parameters", func() {
			So(len(fix.instanceHdlrCalls), ShouldEqual, 1)
			So(fix.instanceHdlrCalls[0], ShouldResemble, newInstanceEvent)
		})

		Convey("And ErrorReporter.Notify is called 1 time with the expected parameters", func() {
			So(len(fix.errorReporter.NotifyCalls()), ShouldEqual, 1)
			So(fix.errorReporter.NotifyCalls()[0].ErrContext, ShouldEqual, "InstanceHandler.Handle returned an unexpected error")
		})

		Convey("And message.Commit is never called", func() {
			So(len(fix.message.CommitCalls()), ShouldEqual, 0)
		})
	})
}

type fixture struct {
	instanceHdlrCalls []event.NewInstance
	instanceHandler   *mock.InstanceEventHandlerMock
	errorReporter     *reportertest.ImportErrorReporterMock
	message           *kafkatest.Message
}

func newFixture(messageBytes []byte, handleInstanceFunc func(e event.NewInstance) error) *fixture {
	instanceHdlrCalls := []event.NewInstance{}

	fix := &fixture{
		instanceHdlrCalls: instanceHdlrCalls,
		message:           kafkatest.NewMessage(messageBytes, 0),
	}

	instanceHandler := &mock.InstanceEventHandlerMock{
		HandleFunc: func(ctx context.Context, e event.NewInstance) error {
			fix.instanceHdlrCalls = append(fix.instanceHdlrCalls, e)
			return handleInstanceFunc(e)
		},
	}

	fix.instanceHandler = instanceHandler
	fix.errorReporter = reportertest.NewImportErrorReporterMock(nil)
	return fix
}
