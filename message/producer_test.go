package message_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/message"
	"github.com/ONSdigital/dp-dimension-importer/message/mock"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/log.go/v2/log"
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
				log.Info(ctx, "avro byte sent to producer output")
			case <-time.After(time.Second * 5):
				log.Info(ctx, "failing test due to timed out")
				t.FailNow()
			}

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)

				Convey("Then the expected bytes are sent to producer.output", func() {
					var actual event.InstanceCompleted
					err := schema.InstanceCompletedSchema.Unmarshal(avroBytes, &actual)
					So(completedEvent, ShouldResemble, actual)
					So(err, ShouldBeNil)
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
				expectedError := fmt.Errorf(fmt.Sprintf("Marshaller.Marshal returned an error: event=%v: %%w", completedEvent), mockError)
				So(err.Error(), ShouldEqual, expectedError.Error())
			})

			Convey("Then producer.Output is never called", func() {
				So(len(kafkaProducerMock.ChannelsCalls()), ShouldEqual, 0)
			})
		})
	})
}
