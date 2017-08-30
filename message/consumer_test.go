package message

import (
	"errors"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/message/message_test"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/kafka"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
	"github.com/ONSdigital/go-ns/log"
)

var (
	errExpected = errors.New("LEEEEROY JENKINS!")
	exitTest    = errors.New("Force test exit")
)

const (
	fileURL           = "test-url"
	instanceID        = "1234567890"
	timeout           = 5
	timeoutFailureMsg = "Concurrent Test did not complete within the configured timeout window. Failing test."
	consumerExitedMsg = "Consumer exited"
	consumerCloserMsg = "consumer closer called"
	producerCloserMsg = "producer closer called"
)

type testCommon struct {
	testComplete         bool
	testCompleteChan     chan error
	incomingChan         chan kafka.Message
	messageCommitInvoked chan int
	consumerErrorsChan   chan error
	producerErrorsChan   chan error
	consumerCloserChan   chan bool
	producerCloserChan   chan bool
	killConsumer         chan error
	extractedEvent       event.DimensionsExtractedEvent
	extractedEventBytes  []byte
	eventHandlerErr      error
}

func TestConsume(t *testing.T) {

	Convey("Given an a valid kafkaMessageMock", t, func() {
		tc := newTestCommon()

		kafkaMessage := tc.kafkaMessageMock()
		eventHandler := tc.eventHandler()
		consumerMock := tc.kafkaMessageConsumerMock()
		insertedProducer := tc.InsertedProducerMock()

		Convey("When the consumer recieves the kafkaMessageMock", func() {
			// run the consumer.
			go runConsumer(tc, consumerMock, insertedProducer, eventHandler)

			// select on the chan's the consumer sends to.
			go func() {
				select {
				case <-time.After(time.Second * timeout):
					fmt.Println(timeoutFailureMsg)
					tc.killConsumer <- exitTest
					t.FailNow()
				case <-tc.messageCommitInvoked:
					fmt.Println("Kafka message committed.")
					tc.killConsumer <- exitTest
				}
			}()

			// send a kafka message to in the inbound chan/
			tc.incomingChan <- kafkaMessage

			// block until the test completes.
			<-tc.testCompleteChan

			Convey("Then eventHandler is called 1 time with the expected parameters", func() {
				calls := eventHandler.Param
				So(len(calls), ShouldEqual, 1)
				So(calls[0], ShouldResemble, tc.extractedEvent)
			})

			Convey("And insertedProducer.DimensionInserted is called 1 time", func() {
				So(len(insertedProducer.DimensionInsertedCalls()), ShouldEqual, 1)

				actual := event.DimensionsInsertedEvent{fileURL, instanceID}
				So(insertedProducer.DimensionInsertedCalls()[0].E, ShouldResemble, actual)
			})

			Convey("And consumedMessage.Commit is called 1 time", func() {
				So(len(kafkaMessage.CommitCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestConsume_InvalidKafkaMessage(t *testing.T) {
	Convey("Given the consumer is configured correctly", t, func() {
		tc := newTestCommon()
		tc.extractedEventBytes = []byte("This is not a valid message")

		invalidKafkaMsg := tc.kafkaMessageMock()
		eventHandler := tc.eventHandler()
		consumerMock := tc.kafkaMessageConsumerMock()
		insertedProducer := tc.InsertedProducerMock()

		// run the consumer.
		go runConsumer(tc, consumerMock, insertedProducer, eventHandler)

		Convey("When an invalid message is recieved", func() {

			consumerMock.Incoming() <- invalidKafkaMsg
			var err error

			// block until either the test times out or the consumer exits
			select {
			case err = <-tc.testCompleteChan:
				fmt.Println(consumerExitedMsg)
			case <-time.After(time.Second * timeout):
				fmt.Println(timeoutFailureMsg)
				t.FailNow()
			}

			Convey("Then the Consumer returns an error", func() {
				So(err, ShouldNotBeNil)
			})

			Convey("And eventHandler.HandleEvent is never called", func() {
				So(len(eventHandler.Param), ShouldEqual, 0)
			})

			Convey("And insertedProducer.DimensionInserted is never called", func() {
				So(len(insertedProducer.DimensionInsertedCalls()), ShouldEqual, 0)
			})

			Convey("And consumedMessage.Commit is never called", func() {
				So(len(invalidKafkaMsg.CommitCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestConsume_EventHandlerError(t *testing.T) {

	Convey("Given the consumer has been configured correctly", t, func() {
		tc := newTestCommon()
		kafkaMessage := tc.kafkaMessageMock()
		consumerMock := tc.kafkaMessageConsumerMock()
		insertedProducer := tc.InsertedProducerMock()

		var err error

		Convey("When eventHandler.DimensionInserted returns an error", func() {
			tc.eventHandlerErr = errExpected
			eventHandler := tc.eventHandler()

			go runConsumer(tc, consumerMock, insertedProducer, eventHandler)

			tc.incomingChan <- kafkaMessage

			select {
			case err = <-tc.testCompleteChan:
				fmt.Println(consumerExitedMsg)
			case <-time.After(time.Second * timeout):
				fmt.Println(timeoutFailureMsg)
				t.FailNow()
			}

			Convey("Then the Consumer returns an error", func() {
				So(err, ShouldNotBeNil)
			})

			Convey("And eventHandler.HandleEvent is called 1 time", func() {
				So(len(eventHandler.Param), ShouldEqual, 1)
			})

			Convey("And insertedProducer.DimensionInserted is never called", func() {
				So(len(insertedProducer.DimensionInsertedCalls()), ShouldEqual, 0)
			})

			Convey("And consumedMessage.Commit is never called", func() {
				So(len(kafkaMessage.CommitCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestConsume_DimensionInsertedError(t *testing.T) {

	Convey("Given the consumer has been configured correctly", t, func() {
		tc := newTestCommon()
		kafkaMessage := tc.kafkaMessageMock()
		consumerMock := tc.kafkaMessageConsumerMock()
		insertedProducer := tc.InsertedProducerMock()
		tc.eventHandlerErr = nil
		eventHandler := tc.eventHandler()

		var err error

		Convey("When insertedProducer.DimensionInserted returns an error", func() {
			insertedProducer.DimensionInsertedFunc = func(e event.DimensionsInsertedEvent) error {
				return errExpected
			}

			go runConsumer(tc, consumerMock, insertedProducer, eventHandler)

			tc.incomingChan <- kafkaMessage

			select {
			case err = <-tc.testCompleteChan:
				fmt.Println(consumerExitedMsg)
			case <-time.After(time.Second * timeout):
				fmt.Println(timeoutFailureMsg)
				t.FailNow()
			}

			Convey("Then the Consumer returns an error", func() {
				So(err, ShouldNotBeNil)
			})

			Convey("And eventHandler.HandleEvent is called 1 time", func() {
				So(len(eventHandler.Param), ShouldEqual, 1)
			})

			Convey("And insertedProducer.DimensionInserted is called 1 time", func() {
				So(len(insertedProducer.DimensionInsertedCalls()), ShouldEqual, 1)
			})

			Convey("And consumedMessage.Commit is never called", func() {
				So(len(kafkaMessage.CommitCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestConsume_dimensionExtractedConsunerError(t *testing.T) {
	Convey("Given the consumer has been configured correctly", t, func() {
		tc := newTestCommon()
		consumerMock := tc.kafkaMessageConsumerMock()
		insertedProducer := tc.InsertedProducerMock()
		tc.eventHandlerErr = nil
		eventHandler := tc.eventHandler()
		var err error

		consumerClosesParams := make([]bool, 0)
		producerClosesParams := make([]bool, 0)

		go runConsumer(tc, consumerMock, insertedProducer, eventHandler)

		tc.consumerErrorsChan <- errExpected

		done := false
		for !done {
			select {
			case c := <-tc.consumerCloserChan:
				consumerClosesParams = append(consumerClosesParams, c)
				fmt.Println(consumerCloserMsg)
			case p := <-tc.producerCloserChan:
				producerClosesParams = append(producerClosesParams, p)
				fmt.Println(producerCloserMsg)
			case err = <-tc.testCompleteChan:
				fmt.Println(consumerExitedMsg)
				done = true
			case <-time.After(time.Second * timeout):
				fmt.Println(timeoutFailureMsg)
				t.FailNow()
			}
		}

		Convey("Then the Consumer returns an error", func() {
			So(err, ShouldNotBeNil)
		})

		Convey("And dimensionExtractedConsumer.Closer() is called 1 time with the expected parameters", func() {
			calls := consumerMock.CloserCalls()
			So(len(calls), ShouldEqual, 1)
			So(len(consumerClosesParams), ShouldEqual, 1)
			So(consumerClosesParams[0], ShouldEqual, true)
		})

		Convey("And insertedProducer.Closer() is called 1 time with the expected parameters", func() {
			calls := insertedProducer.CloserCalls()
			So(len(calls), ShouldEqual, 1)
			So(len(producerClosesParams), ShouldEqual, 1)
			So(producerClosesParams[0], ShouldEqual, true)
		})
	})
}

func TestConsume_insertedProducerError(t *testing.T) {
	Convey("Given the consumer has been configured correctly", t, func() {
		tc := newTestCommon()
		consumerMock := tc.kafkaMessageConsumerMock()
		insertedProducer := tc.InsertedProducerMock()
		tc.eventHandlerErr = nil
		eventHandler := tc.eventHandler()
		var err error

		consumerClosesParams := make([]bool, 0)
		producerClosesParams := make([]bool, 0)

		go runConsumer(tc, consumerMock, insertedProducer, eventHandler)

		tc.producerErrorsChan <- errExpected

		done := false
		for !done {
			select {
			case c := <-tc.consumerCloserChan:
				consumerClosesParams = append(consumerClosesParams, c)
				fmt.Println(consumerCloserMsg)
			case p := <-tc.producerCloserChan:
				producerClosesParams = append(producerClosesParams, p)
				fmt.Println(producerCloserMsg)
			case err = <-tc.testCompleteChan:
				fmt.Println(consumerExitedMsg)
				done = true
			case <-time.After(time.Second * timeout):
				fmt.Println(timeoutFailureMsg)
				t.FailNow()
			}
		}

		Convey("Then the Consumer returns an error", func() {
			So(err, ShouldNotBeNil)
		})

		Convey("And dimensionExtractedConsumer.Closer() is called 1 time with the expected parameters", func() {
			calls := consumerMock.CloserCalls()
			So(len(calls), ShouldEqual, 1)
			So(len(consumerClosesParams), ShouldEqual, 1)
			So(consumerClosesParams[0], ShouldEqual, true)
		})

		Convey("And insertedProducer.Closer() is called 1 time with the expected parameters", func() {
			calls := insertedProducer.CloserCalls()
			So(len(calls), ShouldEqual, 1)
			So(len(producerClosesParams), ShouldEqual, 1)
			So(producerClosesParams[0], ShouldEqual, true)
		})
	})
}

func runConsumer(tc *testCommon, consumerMock *message_test.KafkaMessageConsumerMock, insertedProducer InsertedProducer, handler EventHandler) {
	// Block until the Consumer returns an error (its exited) and send that error to the test complete chan.
	tc.testCompleteChan <- Consume(consumerMock, insertedProducer, handler, tc.killConsumer)
	log.Debug(consumerExitedMsg, nil)
}

func newTestCommon() *testCommon {
	dimensionsExtractedEvent := event.DimensionsExtractedEvent{
		FileURL:    fileURL,
		InstanceID: instanceID,
	}

	bytes, _ := schema.DimensionsExtractedSchema.Marshal(dimensionsExtractedEvent)

	return &testCommon{
		testCompleteChan:     make(chan error),
		incomingChan:         make(chan kafka.Message),
		messageCommitInvoked: make(chan int),
		consumerErrorsChan:   make(chan error),
		consumerCloserChan:   make(chan bool),
		producerCloserChan:   make(chan bool),
		producerErrorsChan:   make(chan error),
		killConsumer:         make(chan error),
		extractedEvent:       dimensionsExtractedEvent,
		extractedEventBytes:  bytes,
		eventHandlerErr:      nil,
	}
}

func (tc *testCommon) eventHandler() *message_test.EventHandlerMock {
	return &message_test.EventHandlerMock{
		Param: []event.DimensionsExtractedEvent{},
		HandleEventFunc: func(e event.DimensionsExtractedEvent) error {
			return tc.eventHandlerErr
		},
	}
}

func (tc *testCommon) kafkaMessageMock() *message_test.KafkaMessageMock {
	return &message_test.KafkaMessageMock{
		GetDataFunc: func() []byte {
			return tc.extractedEventBytes
		},
		CommitFunc: func() {
			// capture commit invoked.
			tc.messageCommitInvoked <- 1
		},
	}
}

func (tc *testCommon) kafkaMessageConsumerMock() *message_test.KafkaMessageConsumerMock {
	return &message_test.KafkaMessageConsumerMock{
		IncomingFunc: func() chan kafka.Message {
			return tc.incomingChan
		},
		CloserFunc: func() chan bool {
			return tc.consumerCloserChan
		},
		ErrorsFunc: func() chan error {
			return tc.consumerErrorsChan
		},
	}
}

func (tc *testCommon) InsertedProducerMock() *message_test.InsertedProducerMock {
	return &message_test.InsertedProducerMock{
		DimensionInsertedFunc: func(e event.DimensionsInsertedEvent) error {
			return nil
		},
		ErrorsFunc: func() chan error {
			return tc.producerErrorsChan
		},
		CloserFunc: func() chan bool {
			return tc.producerCloserChan
		},
	}
}
