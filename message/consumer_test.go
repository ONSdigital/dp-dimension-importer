package message_test

import (
	"sync"
	"testing"

	"github.com/ONSdigital/dp-dimension-importer/config"
	"github.com/ONSdigital/dp-dimension-importer/message"
	mock "github.com/ONSdigital/dp-dimension-importer/message/mock"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConsumer_Listen(t *testing.T) {

	cfg := &config.Config{
		KafkaNumWorkers: 1,
	}

	Convey("Given the Consumer has been configured correctly", t, func() {

		cgChannels := kafka.CreateConsumerGroupChannels(1)
		kafkaConsumer := &kafkatest.IConsumerGroupMock{
			ChannelsFunc: func() *kafka.ConsumerGroupChannels { return cgChannels },
		}

		handlerWg := &sync.WaitGroup{}
		receiverMock := &mock.ReceiverMock{
			OnMessageFunc: func(message kafka.Message) {
				handlerWg.Done()
			},
		}

		Convey("When the consumer receives a valid message", func() {

			msg := kafkatest.NewMessage([]byte{1, 2, 3, 4, 5}, 0)
			kafkaConsumer.Channels().Upstream <- msg

			handlerWg.Add(1)
			message.Consume(ctx, kafkaConsumer, receiverMock, cfg)
			handlerWg.Wait()

			Convey("OnMessage is called on the receiver ", func() {
				So(receiverMock.OnMessageCalls(), ShouldHaveLength, 1)
				So(receiverMock.OnMessageCalls()[0].Message, ShouldEqual, msg)
			})

			Convey("The message consumer is released", func() {
				<-msg.UpstreamDone()
				So(msg.CommitAndReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})
}
