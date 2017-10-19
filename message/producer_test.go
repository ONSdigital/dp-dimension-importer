package message

import (
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/event"
	mock "github.com/ONSdigital/dp-dimension-importer/message/message_test"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var completedEvent = event.InstanceCompleted{
	InstanceID: "1234567890",
	FileURL:    "/cmd/my.csv",
}

func TestInstanceCompletedProducer_Completed(t *testing.T) {
	Convey("Given InstanceCompletedProducer has been configured correctly", t, func() {
		output := make(chan []byte, 1)

		kafkaProducerMock := &mock.KafkaProducerMock{
			OutputFunc: func() chan []byte {
				return output
			},
		}
		marshallerMock := &mock.MarshallerMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return schema.InstanceCompletedSchema.Marshal(s)
			},
		}

		instanceCompletedProducer := InstanceCompletedProducer{
			Producer:   kafkaProducerMock,
			Marshaller: marshallerMock,
		}

		Convey("When given a valid instanceCompletedEvent", func() {

			err := instanceCompletedProducer.Completed(completedEvent)

			var avroBytes []byte
			select {
			case avroBytes = <-output:
				log.Info("Avro byte sent to producer output", nil)
			case <-time.After(time.Second * 5):
				log.Info("Failing test due to timed out", nil)
				t.FailNow()
			}

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)

				Convey("And the exepcted bytes are sent to producer.output", func() {
					var actual event.InstanceCompleted
					schema.InstanceCompletedSchema.Unmarshal(avroBytes, &actual)
					So(completedEvent, ShouldResemble, actual)
				})
			})
		})
	})
}

func TestInstanceCompletedProducer_Completed_MarshalErr(t *testing.T) {
	Convey("Given InstanceCompletedProducer has been configured correctly", t, func() {
		output := make(chan []byte)
		mockError := errors.New("mock error")

		kafkaProducerMock := &mock.KafkaProducerMock{
			OutputFunc: func() chan []byte {
				return output
			},
		}
		marshallerMock := &mock.MarshallerMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return nil, mockError
			},
		}

		instanceCompletedProducer := InstanceCompletedProducer{
			Producer:   kafkaProducerMock,
			Marshaller: marshallerMock,
		}

		Convey("When marshaller.Marshal returns an error", func() {
			err := instanceCompletedProducer.Completed(completedEvent)

			Convey("Then the expected error is returned", func() {
				expectedError := errors.Wrap(mockError, fmt.Sprintf("Marshaller.Marshal returned an error: event=%v", completedEvent))
				So(err.Error(), ShouldEqual, expectedError.Error())
			})

			Convey("And producer.Output is never called", func() {
				So(len(kafkaProducerMock.OutputCalls()), ShouldEqual, 0)
			})
		})
	})
}
