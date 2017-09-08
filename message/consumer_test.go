package message

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	mock "github.com/ONSdigital/dp-dimension-importer/message/message_test"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/log"
	"time"
	"errors"
)

type testCommon struct {
	incomingChan     chan kafka.Message
	committedChan    chan bool
	extractedEvent   event.NewInstanceEvent
	insertedEvent    event.InstanceCompletedEvent
	message          *mock.MessageMock
	consumerMock     *mock.KafkaConsumerMock
	producerMock     *mock.CompletedProducerMock
	eventHandlerMock *mock.EventHandlerMock
	errorHandlerMock *mock.ErrorEventHandlerMock
}

func TestConsume(t *testing.T) {
	Convey("Given consumer has been correctly configiured", t, func() {
		tc := newtestCommon()

		// Run the Consumer
		Consume(tc.consumerMock, tc.producerMock, tc.eventHandlerMock, tc.errorHandlerMock)

		Convey("When consumer receieves a valid incoming message", func() {

			tc.consumerMock.Incoming() <- tc.message
			blockOnCommitOrTimeout(t, tc)

			Convey("Then eventHanlder is called 1 time with the correct parameters", func() {
				calls := tc.eventHandlerMock.Param
				So(len(calls), ShouldEqual, 1)
				So(calls[0], ShouldResemble, tc.extractedEvent)
			})

			Convey("And producer.Completed is called 1 time with the correct parameters", func() {
				calls := tc.producerMock.CompletedCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].E, ShouldResemble, tc.insertedEvent)
			})
		})
	})

}

func TestConsume_invalidKafkaMessage(t *testing.T) {
	Convey("Given consumer has been correctly configiured", t, func() {
		tc := newtestCommon()

		// run the consumer.
		Consume(tc.consumerMock, tc.producerMock, tc.eventHandlerMock, tc.errorHandlerMock)

		Convey("When an invalid kafka message is receieved", func() {
			invalidBytes := []byte("I am invalid")

			tc.message = &mock.MessageMock{
				Committed: tc.committedChan,
				Data:      invalidBytes,
			}

			tc.consumerMock.Incoming() <- tc.message
			blockOnCommitOrTimeout(t, tc)

			Convey("Then errorHandler should be called 1 time with the expected parameters", func() {
				calls := tc.errorHandlerMock.HandleCalls()
				So(len(calls), ShouldEqual, 1)
			})

			Convey("And eventHandler.Handle is never called", func() {
				So(len(tc.eventHandlerMock.Param), ShouldEqual, 0)
			})
		})
	})
}

func TestConsume_eventHandlerError(t *testing.T) {
	Convey("Given consumer has been correctly configiured", t, func() {
		expectedErr := errors.New("Expected error")
		tc := newtestCommon()
		tc.eventHandlerMock.HandleEventFunc = func(event event.NewInstanceEvent) error {
			return expectedErr
		}

		// run the consumer.
		Consume(tc.consumerMock, tc.producerMock, tc.eventHandlerMock, tc.errorHandlerMock)

		Convey("When eventHandler.HandleEvent returns an error", func() {
			tc.consumerMock.Incoming() <- tc.message
			blockOnCommitOrTimeout(t, tc)

			Convey("Then errorHandler.Handle is called 1 time with the correct parameters", func() {
				calls := tc.errorHandlerMock.HandleCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].InstanceID, ShouldEqual, tc.extractedEvent.InstanceID)
				So(calls[0].Err, ShouldResemble, expectedErr)
				So(calls[0].Data, ShouldBeNil)
			})

			Convey("And producer.Completed is never called", func() {
				So(len(tc.producerMock.CompletedCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestConsume_dimensionInsertedError(t *testing.T) {
	Convey("Given consumer has been correctly configiured", t, func() {
		expectedErr := errors.New("Expected error")

		tc := newtestCommon()
		tc.producerMock.CompletedFunc = func(e event.InstanceCompletedEvent) error {
			return expectedErr
		}

		// run the consumer.
		Consume(tc.consumerMock, tc.producerMock, tc.eventHandlerMock, tc.errorHandlerMock)

		Convey("When producer.Completed returns an error", func() {
			tc.consumerMock.Incoming() <- tc.message
			blockOnCommitOrTimeout(t, tc)

			Convey("Then eventHandler.HandleEvent is called 1 time with the correct parameters", func() {
				So(len(tc.eventHandlerMock.Param), ShouldEqual, 1)
				So(tc.eventHandlerMock.Param[0], ShouldResemble, tc.extractedEvent)
			})

			Convey("Then producer.Completed is called 1 time with the correct parameters", func() {
				calls := tc.producerMock.CompletedCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].E, ShouldResemble, tc.insertedEvent)
			})

			Convey("And errorHandler.Handle is called 1 time with the correct parameters", func() {
				calls := tc.errorHandlerMock.HandleCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].InstanceID, ShouldEqual, tc.extractedEvent.InstanceID)
				So(calls[0].Err, ShouldResemble, expectedErr)
				So(calls[0].Data, ShouldBeNil)
			})
		})
	})
}

func blockOnCommitOrTimeout(t *testing.T, tc *testCommon) {
	select {
	case <-tc.committedChan:
		log.Debug("Message committed", nil)
	case <-time.After(time.Second * 3):
		log.Debug("Test has timed out", nil)
		t.FailNow()
	}

	CloseConsumer()
}

func newtestCommon() *testCommon {
	incomingChan := make(chan kafka.Message)
	committedChan := make(chan bool)

	extractedEvent := event.NewInstanceEvent{
		InstanceID: "1234567890",
		FileURL:    "/my.csv",
	}

	b, _ := schema.NewInstanceSchema.Marshal(extractedEvent)

	insertedEvent := event.InstanceCompletedEvent{
		InstanceID: extractedEvent.InstanceID,
		FileURL:    extractedEvent.FileURL,
	}

	message := &mock.MessageMock{
		Committed: committedChan,
		Data:      b,
	}

	consumerMock := &mock.KafkaConsumerMock{
		IncomingFunc: func() chan kafka.Message {
			return incomingChan
		},
	}

	producerMock := &mock.CompletedProducerMock{
		CompletedFunc: func(e event.InstanceCompletedEvent) error {
			return nil
		},
	}
	eventHandlerMock := &mock.EventHandlerMock{
		Param: make([] event.NewInstanceEvent, 0),
		HandleEventFunc: func(event event.NewInstanceEvent) error {
			return nil
		},
	}

	errorHandlerMock := &mock.ErrorEventHandlerMock{
		HandleFunc: func(instanceID string, err error, data log.Data) {
		},
	}

	return &testCommon{
		incomingChan:     incomingChan,
		committedChan:    committedChan,
		extractedEvent:   extractedEvent,
		insertedEvent:    insertedEvent,
		message:          message,
		consumerMock:     consumerMock,
		producerMock:     producerMock,
		eventHandlerMock: eventHandlerMock,
		errorHandlerMock: errorHandlerMock,
	}
}
