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
)

var errExpected = errors.New("LEEEEROY JENKINS!")

const (
	fileURL    = "test-url"
	instanceID = "1234567890"
	timeout    = 5
)

type testCommon struct {
	incomingChan        chan kafka.Message
	errorsChan          chan error
	closerChan          chan bool
	producerChan        chan []byte
	exitChan            chan error
	extractedEvent      event.DimensionsExtractedEvent
	extractedEventBytes []byte
	eventHandler        *message_test.EventHandlerMock
}

func TestConsume(t *testing.T) {

	Convey("Given an a valid kafkaMessage", t, func() {
		tc := newTestCommon()
		kafkaMessage := newMockKafkaMessage(tc)
		consumerMock := newKafkaMessageConsumerMock(tc)
		producerMock := newKafkaMessageProducerMock(tc)
		marshallerMock := newMarshallerMock(tc)
		dimensionInsertedProducer := newDimensionInsertedProducer(marshallerMock, producerMock)

		done := false
		var producedBytes [][]byte

		Convey("When the consumer recieves the kafkaMessage", func() {
			go Consume(consumerMock, dimensionInsertedProducer, tc.eventHandler, tc.exitChan)

			tc.incomingChan <- kafkaMessage

			// Without blocking here the test finishes before the consumer can do anything with the input which fails
			// the test assertions. So block until the the Producer outputs some bytes or timeout after the configured
			// time frame.
			for !done {
				select {
				case <-time.After(time.Second * timeout):
					fmt.Println("Test failed: chan 'producerChan' did not return anything within the timeout windown. Failing test")
					tc.exitChan <- errors.New("ForcedExit")
					t.FailNow()
				case out := <-tc.producerChan:
					fmt.Println("Producer has output bytes.")
					producedBytes = append(producedBytes, out)
					done = true
				}
			}

			Convey("Then eventHandler is called 1 time with the expected parameters", func() {
				calls := tc.eventHandler.Param
				So(len(calls), ShouldEqual, 1)

				So(calls[0], ShouldResemble, tc.extractedEvent)

				Convey("And the Producer is called 1 time and produces the expected output", func() {
					So(len(marshallerMock.MarshalCalls()), ShouldEqual, 1)
					So(len(producedBytes), ShouldEqual, 1)

					insteredEvent := event.DimensionsInsertedEvent{fileURL, instanceID}
					expected, _ := schema.DimensionsInsertedSchema.Marshal(insteredEvent)
					So(producedBytes[0], ShouldResemble, expected)

					Convey("And consumedMessage.Commit is called 1 time.", func() {
						So(len(kafkaMessage.CommitCalls()), ShouldEqual, 1)
					})
				})
			})

			// Use the exit channel to end the go rout
			tc.exitChan <- errors.New("Test completed.")
		})
	})
}

func TestConsume_EventHandlerError(t *testing.T) {
	Convey("Given the consumer has been configured correctly", t, func() {
		tc := newTestCommon()
		kafkaMessage := newMockKafkaMessage(tc)
		consumerMock := newKafkaMessageConsumerMock(tc)
		producerMock := newKafkaMessageProducerMock(tc)
		consumerReturn := make(chan error)
		var err error
		producerOutput := make([][]byte, 0)
		marshallerMock := newMarshallerMock(tc)
		dimensionInsertedProducer := newDimensionInsertedProducer(marshallerMock, producerMock)

		tc.eventHandler.HandleEventFunc = func(e event.DimensionsExtractedEvent) error {
			return errExpected
		}

		Convey("When eventHandler.DimensionInserted returns an error", func() {
			go func() {
				consumerReturn <- Consume(consumerMock, dimensionInsertedProducer, tc.eventHandler, tc.exitChan)
			}()

			tc.incomingChan <- kafkaMessage
			select {
			case <-time.After(time.Second * timeout):
				fmt.Println("Test failed: chan 'consumerReturn' failed to return an error within the timeout window. Forcing test to fail.")
				tc.exitChan <- errors.New("ForcedExit")
				t.FailNow()
			case err = <-consumerReturn:
				fmt.Println("Consumer has exited as expected.")
			case out := <-tc.producerChan:
				producerOutput = append(producerOutput, out)
			}

			Convey("Then the expected error is sent to the exit channel", func() {
				So(err, ShouldResemble, errExpected)

			})

			Convey("And eventHandler.DimensionInserted is only invoked 1 time", func() {
				So(len(tc.eventHandler.Param), ShouldEqual, 1)
			})

			Convey("And Producer is never invoked.", func() {
				So(len(producerOutput), ShouldEqual, 0)
				So(len(marshallerMock.MarshalCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestConsume_InvalidKafkaMessage(t *testing.T) {

	Convey("Given an a invalid kafkaMessage", t, func() {
		// Set up mocks.
		invalidBytes := []byte("This is not a valid message")
		producerOutput := make([][]byte, 0)

		tc := newTestCommon()
		consumerMock := newKafkaMessageConsumerMock(tc)
		producerMock := newKafkaMessageProducerMock(tc)
		marshallerMock := newMarshallerMock(tc)
		dimensionInsertedProducer := newDimensionInsertedProducer(marshallerMock, producerMock)

		invalidKafkaMsg := newMockKafkaMessage(tc)
		invalidKafkaMsg.GetDataFunc = func() []byte {
			return invalidBytes
		}

		// Run test.
		Convey("When the message is passed to the Consumer", func() {
			consumerReturn := make(chan error)
			var err error
			go func() {
				consumerReturn <- Consume(consumerMock, dimensionInsertedProducer, tc.eventHandler, tc.exitChan)
			}()

			tc.incomingChan <- invalidKafkaMsg

			select {
			case <-time.After(time.Second * timeout):
				fmt.Println("Test failed: chan 'consumerReturn' failed to return an error within the timeout window. Forcing test to fail.")
				tc.exitChan <- errors.New("ForcedExit")
				t.FailNow()
			case err = <-consumerReturn:
				fmt.Println("Consumer has exited as expected.")
			case out := <-tc.producerChan:
				producerOutput = append(producerOutput, out)
			}

			Convey("Then an error is sent to the exit channel", func() {
				So(err, ShouldNotBeNil)

				Convey("And eventHandler.DimensionInserted is never invoked", func() {
					So(len(tc.eventHandler.Param), ShouldEqual, 0)
				})

				Convey("And the Producer is never invoked", func() {
					So(len(producerOutput), ShouldEqual, 0)
					So(len(marshallerMock.MarshalCalls()), ShouldEqual, 0)
				})
			})
		})
	})
}

func newTestCommon() *testCommon {
	dimensionsExtractedEvent := event.DimensionsExtractedEvent{
		FileURL:    fileURL,
		InstanceID: instanceID,
	}

	bytes, _ := schema.DimensionsExtractedSchema.Marshal(dimensionsExtractedEvent)

	return &testCommon{
		incomingChan:        make(chan kafka.Message, 0),
		errorsChan:          make(chan error, 0),
		closerChan:          make(chan bool, 0),
		producerChan:        make(chan []byte),
		exitChan:            make(chan error),
		extractedEvent:      dimensionsExtractedEvent,
		extractedEventBytes: bytes,
		eventHandler: &message_test.EventHandlerMock{
			Param: []event.DimensionsExtractedEvent{},
			HandleEventFunc: func(e event.DimensionsExtractedEvent) error {
				return nil
			},
		},
	}
}

func newMockKafkaMessage(tc *testCommon) *message_test.KafkaMessageMock {
	return &message_test.KafkaMessageMock{
		GetDataFunc: func() []byte {
			return tc.extractedEventBytes
		},
		CommitFunc: func() {
		},
	}
}

func newKafkaMessageConsumerMock(tc *testCommon) *message_test.KafkaMessageConsumerMock {
	return &message_test.KafkaMessageConsumerMock{
		IncomingFunc: func() chan kafka.Message {
			return tc.incomingChan
		},
		CloserFunc: func() chan bool {
			return tc.closerChan
		},
		ErrorsFunc: func() chan error {
			return tc.errorsChan
		},
	}
}
func newKafkaMessageProducerMock(tc *testCommon) *message_test.KafkaMessageProducerMock {
	return &message_test.KafkaMessageProducerMock{
		ErrorsFunc: func() chan error {
			return tc.errorsChan
		},
		CloserFunc: func() chan bool {
			return tc.closerChan
		},
		OutputFunc: func() chan []byte {
			return tc.producerChan
		},
	}
}

func newMarshallerMock(tc *testCommon) *message_test.MarshallerMock {
	return &message_test.MarshallerMock{
		MarshalFunc: func(s interface{}) ([]byte, error) {
			return tc.extractedEventBytes, nil
		},
	}
}

func newDimensionInsertedProducer(m *message_test.MarshallerMock, p *message_test.KafkaMessageProducerMock) DimensionInsertedProducer {
	return DimensionInsertedProducer{
		Marshaller: m,
		Producer:   p,
	}
}
