package message

import (
	"context"
	"time"

	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/go-ns/kafka"
)

//go:generate moq -out ./message_test/consumer_generated_mocks.go -pkg message_test . KafkaMessage KafkaConsumer

var loggerC = logging.Logger{Name: "message.Consumer"}

// KafkaMessage type representing a kafka message.
type KafkaMessage kafka.Message

// KafkaConsumer consume an incoming kafka message
type KafkaConsumer interface {
	Incoming() chan kafka.Message
}

// Reciever is sent a kafka messages and processes it
type Reciever interface {
	OnMessage(message kafka.Message)
}

// Consumer listens for kafka messages on the specified topic, processes thems & sends an outbound kafka message when complete.
type Consumer struct {
	closed          chan bool
	ctx             context.Context
	cancel          context.CancelFunc
	consumer        KafkaConsumer
	messageReciever Reciever
	defaultShutdown time.Duration
}

// NewConsumer create a NewInstance event consumer.
func NewConsumer(consumer KafkaConsumer, messageReciever Reciever, defaultShutdown time.Duration) Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	return Consumer{
		closed:          make(chan bool, 1),
		ctx:             ctx,
		cancel:          cancel,
		consumer:        consumer,
		messageReciever: messageReciever,
		defaultShutdown: defaultShutdown,
	}
}

// Listen poll the KafkaConsumer for incoming messages and pass onto the Reciever
func (c Consumer) Listen() {
	go func() {
		defer func() {
			// Notify that the consumer loop has closed.
			c.closed <- true
		}()

		for {
			select {
			case consumedMessage := <-c.consumer.Incoming():
				loggerC.Info("consumer.incoming receieved a message", nil)
				c.messageReciever.OnMessage(consumedMessage)
			case <-c.ctx.Done():
				loggerC.Info("loggercontext.Done received event, attempting to close consumer", nil)
				return
			}
		}
	}()
}

// Close shutdown Consumer.Listen loop.
func (c Consumer) Close(ctx context.Context) {
	// if nil use a default context with a timeout
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.defaultShutdown)
		defer cancel()
	}

	// if the context is not nil but no deadline is set the apply the default
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.defaultShutdown)
		defer cancel()
	}

	// Call cancel to attempt to exit the consumer loop.
	c.cancel()

	// Wait for the consumer to tell is has exited or the context timeout occurs.
	select {
	case <-c.closed:
		loggerC.Info("gracefully shutdown message.Consumer", nil)
	case <-ctx.Done():
		loggerC.Info("forced shutdown of message.Consumer", nil)
	}
}
