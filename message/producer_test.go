package message_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/message"
	mock "github.com/ONSdigital/dp-dimension-importer/message/mock"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/log.go/log"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var completedEvent = event.InstanceCompleted{
	InstanceID: "1234567890",
	FileURL:    "/cmd/my.csv",
}

var ctx = context.Background()

func TestInstanceCompletedProducer_Completed(t *testing.T) {
	Convey("Given InstanceCompletedProducer has been configured correctly", t, func() {
		pChannels := &kafka.ProducerChannels{
			Output: make(chan []byte, 1),
		}

		kafkaProducerMock := &kafkatest.IProducerMock{
			ChannelsFunc: func() *kafka.ProducerChannels {
				return pChannels
			},
		}
		marshallerMock := &mock.MarshallerMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return schema.InstanceCompletedSchema.Marshal(s)
			},
		}

		instanceCompletedProducer := message.InstanceCompletedProducer{
			Producer:   kafkaProducerMock,
			Marshaller: marshallerMock,
		}

		Convey("When given a valid instanceCompletedEvent", func() {

			err := instanceCompletedProducer.Completed(ctx, completedEvent)

			var avroBytes []byte
			select {
			case avroBytes = <-pChannels.Output:
				log.Event(ctx, "avro byte sent to producer output", log.INFO)
			case <-time.After(time.Second * 5):
				log.Event(ctx, "failing test due to timed out", log.INFO)
				t.FailNow()
			}

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)

				Convey("And the expected bytes are sent to producer.output", func() {
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
		pChannels := &kafka.ProducerChannels{
			Output: make(chan []byte),
		}
		mockError := errors.New("mock error")

		kafkaProducerMock := &kafkatest.IProducerMock{
			ChannelsFunc: func() *kafka.ProducerChannels {
				return pChannels
			},
		}
		marshallerMock := &mock.MarshallerMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return nil, mockError
			},
		}

		instanceCompletedProducer := message.InstanceCompletedProducer{
			Producer:   kafkaProducerMock,
			Marshaller: marshallerMock,
		}

		Convey("When marshaller.Marshal returns an error", func() {
			err := instanceCompletedProducer.Completed(ctx, completedEvent)

			Convey("Then the expected error is returned", func() {
				expectedError := errors.Wrap(mockError, fmt.Sprintf("Marshaller.Marshal returned an error: event=%v", completedEvent))
				So(err.Error(), ShouldEqual, expectedError.Error())
			})

			Convey("And producer.Output is never called", func() {
				So(len(kafkaProducerMock.ChannelsCalls()), ShouldEqual, 0)
			})
		})
	})
}
