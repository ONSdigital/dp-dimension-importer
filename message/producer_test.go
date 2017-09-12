package message

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	mock "github.com/ONSdigital/dp-dimension-importer/message/message_test"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"github.com/ONSdigital/go-ns/log"
	"time"
	"errors"
)

var completedEvent = event.InstanceCompletedEvent{
	InstanceID: "1234567890",
	FileURL:    "/cmd/my.csv",
}

func TestInstanceCompletedProducer_Completed(t *testing.T) {
	Convey("Given InstanceCompletedProducer has been configured correctly", t, func() {
		output := make(chan []byte)

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
			Producer: kafkaProducerMock,
			Marshaller: marshallerMock,
		}

		Convey("When given a valid instanceCompletedEvent", func() {

			var avroBytes []byte
			go func() {
				select {
				case avroBytes = <-output:
					log.Info("Avro byte sent to producer output", nil)
				case <-time.After(time.Second * 5):
					log.Info("Failing test due to timed out", nil)
					t.FailNow()
				}
			}()

			err := instanceCompletedProducer.Completed(completedEvent)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)

				Convey("And the exepcted bytes are sent to producer.output", func() {
					var actual event.InstanceCompletedEvent
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
		expectedErr := errors.New("Boom!")

		kafkaProducerMock := &mock.KafkaProducerMock{
			OutputFunc: func() chan []byte {
				return output
			},
		}
		marshallerMock := &mock.MarshallerMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return nil, expectedErr
			},
		}

		instanceCompletedProducer := InstanceCompletedProducer{
			Producer: kafkaProducerMock,
			Marshaller: marshallerMock,
		}

		Convey("When marshaller.Marshal returns an error", func() {
			err := instanceCompletedProducer.Completed(completedEvent)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, expectedErr)
			})

			Convey("And producer.Output is never called", func() {
				So(len(kafkaProducerMock.OutputCalls()), ShouldEqual, 0)
			})
		})
	})
}
