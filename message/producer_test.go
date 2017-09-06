package message

import (
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/message/message_test"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestDimensionInsertedProducer_DimensionInserted(t *testing.T) {
	Convey("Given a DimensionInsertProducer with valid configuration", t, func() {
		outputChan := make(chan []byte)
		closerChan := make(chan bool)
		errorsChan := make(chan error)

		insertedEvent := event.DimensionsInsertedEvent{
			FileURL:    fileURL,
			InstanceID: instanceID,
		}

		eventBytes, _ := schema.DimensionsInsertedSchema.Marshal(insertedEvent)

		marshallerMock := &message_test.MarshallerMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return eventBytes, nil
			},
		}

		producerMock := &message_test.KafkaMessageProducerMock{
			OutputFunc: func() chan []byte {
				return outputChan
			},
			CloserFunc: func() chan bool {
				return closerChan
			},
			ErrorsFunc: func() chan error {
				return errorsChan
			},
		}

		target := &DimensionInsertedProducer{
			Marshaller: marshallerMock,
			Producer:   producerMock,
		}

		done := false
		output := make([]byte, 0)
		go func() {
			for !done {
				select {
				case bytes := <-outputChan:
					output = bytes
					done = true
				case <-time.After(time.Second * 10):
					done = true
					t.Fail()
				}
			}
		}()

		Convey("When DimensionInserted is invoked with valid parameters", func() {
			err := target.DimensionInserted(insertedEvent)

			Convey("Then, no error is returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("And the expected data is sent to the Producer.Output chanel", func() {
				So(len(producerMock.OutputCalls()), ShouldEqual, 1)
				So(output, ShouldResemble, eventBytes)
			})

			Convey("And marshal is called 1 time with the expected parameters.", func() {
				So(len(marshallerMock.MarshalCalls()), ShouldEqual, 1)
			})

			Convey("And no other calls to the Producer are made.", func() {
				So(len(producerMock.ErrorsCalls()), ShouldEqual, 0)
				So(len(producerMock.CloserCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestDimensionInsertedProducer_DimensionInserted_MarshalErr(t *testing.T) {
	Convey("Given a DimensionInsertProducer with valid configuration", t, func() {
		outputChan := make(chan []byte)
		closerChan := make(chan bool)
		errorsChan := make(chan error)

		insertedEvent := event.DimensionsInsertedEvent{
			FileURL:    fileURL,
			InstanceID: instanceID,
		}

		marshallerMock := &message_test.MarshallerMock{}

		producerMock := &message_test.KafkaMessageProducerMock{
			OutputFunc: func() chan []byte {
				return outputChan
			},
			CloserFunc: func() chan bool {
				return closerChan
			},
			ErrorsFunc: func() chan error {
				return errorsChan
			},
		}

		target := &DimensionInsertedProducer{
			Marshaller: marshallerMock,
			Producer:   producerMock,
		}

		Convey("When the Marshaller returns an error", func() {
			avroErr := errors.New("Avro error")

			marshallerMock.MarshalFunc = func(s interface{}) ([]byte, error) {
				return nil, avroErr
			}

			err := target.DimensionInserted(insertedEvent)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, avroErr)
			})

			Convey("And the Marshaller is called 1 time with expected parameters", func() {
				So(len(marshallerMock.MarshalCalls()), ShouldEqual, 1)
			})

			Convey("And there are no interactions with the Producer", func() {
				So(len(producerMock.OutputCalls()), ShouldEqual, 0)
				So(len(producerMock.ErrorsCalls()), ShouldEqual, 0)
				So(len(producerMock.CloserCalls()), ShouldEqual, 0)
			})

		})
	})
}
