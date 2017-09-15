package message

import (
	"errors"
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	mock "github.com/ONSdigital/dp-dimension-importer/message/message_test"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"time"
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
	producerCloserMsg = "Producer closer called"
)

func TestConsumer_Listen(t *testing.T) {
	Convey("Given the Consumer has been configured correctly", t, func() {
		incoming := make(chan kafka.Message, 1)
		handlerInvoked := make(chan kafka.Message, 1)

		kafkaConsumer := &mock.KafkaConsumerMock{
			IncomingFunc: func() chan kafka.Message {
				return incoming
			},
		}

		handleCalls := make([]kafka.Message, 0)
		msgHdlr := mock.MessageHandler{
			HandleFunc: func(message kafka.Message) {
				handleCalls = append(handleCalls, message)
				handlerInvoked <- message
			},
		}

		msg := &mock.KafkaMessageMock{}

		consumer := NewConsumer(kafkaConsumer, msgHdlr)
		consumer.Listen()

		Convey("When the consumer receieves a valid message", func() {
			incoming <- msg

			select {
			case <-handlerInvoked:
				log.Info("Handler invoked", nil)
			case <-time.After(time.Second * 3):
				log.Info("Test timed out.", nil)
				t.FailNow()
			}
			consumer.Close(nil)

			Convey("Then messageHandler.Handle is called 1 time with the expected parameters", func() {
				So(len(handleCalls), ShouldEqual, 1)
			})
		})

	})
}
