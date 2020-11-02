package message_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/message"
	mock "github.com/ONSdigital/dp-dimension-importer/message/mock"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/log.go/log"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	errExpected   = errors.New("bork")
	errExitedTest = errors.New("forced test exit")
)

const (
	fileURL           = "test-url"
	instanceID        = "1234567890"
	timeout           = 5
	timeoutFailureMsg = "Concurrent Test did not complete within the configured timeout window. Failing test."
	consumerExitedMsg = "Consumer exited"
	consumerCloserMsg = "consumer closer called"
	producerCloserMsg = "Producer closer called"
)

func TestConsumer_Listen(t *testing.T) {
	Convey("Given the Consumer has been configured correctly", t, func() {
		cgChannels := kafka.CreateConsumerGroupChannels(1)
		handlerInvoked := make(chan kafka.Message, 1)

		kafkaConsumer := &kafkatest.IConsumerGroupMock{
			ChannelsFunc: func() *kafka.ConsumerGroupChannels { return cgChannels },
		}

		handleCalls := []kafka.Message{}
		receiverMock := &mock.ReceiverMock{
			OnMessageFunc: func(message kafka.Message) {
				handleCalls = append(handleCalls, message)
				handlerInvoked <- message
			},
		}

		msg := &kafkatest.MessageMock{
			CommitFunc: func() {

			},
		}

		consumer := message.NewConsumer(ctx, kafkaConsumer, receiverMock, time.Second*10)
		consumer.Listen()

		Convey("When the consumer receives a valid message", func() {
			cgChannels.Upstream <- msg

			select {
			case <-handlerInvoked:
				log.Event(ctx, "handler invoked", log.INFO)
			case <-time.After(time.Second * 3):
				log.Event(ctx, "test timed out.", log.INFO)
				t.FailNow()
			}
			consumer.Close(ctx)

			Convey("Then messageReceiver.OnMessage is called 1 time with the expected parameters", func() {
				So(len(handleCalls), ShouldEqual, 1)
				So(len(msg.CommitCalls()), ShouldEqual, 1)
			})
		})

	})
}
