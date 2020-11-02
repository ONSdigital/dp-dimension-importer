package message

import (
	"context"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out mock/receiver.go -pkg mock . Receiver

var packageName = "handler.InstanceEventHandler"

// KafkaMessage type representing a kafka message.
type KafkaMessage kafka.Message

// Receiver is sent a kafka messages and processes it
type Receiver interface {
	OnMessage(message kafka.Message)
}

// Consumer listens for kafka messages on the specified topic, processes thems & sends an outbound kafka message when complete.
type Consumer struct {
	closed          chan bool
	ctx             context.Context
	cancel          context.CancelFunc
	consumer        kafka.IConsumerGroup
	messageReceiver Receiver
	defaultShutdown time.Duration
}

// NewConsumer create a NewInstance event consumer.
func NewConsumer(ctx context.Context, consumer kafka.IConsumerGroup, messageReceiver Receiver, defaultShutdown time.Duration) Consumer {
	ctx, cancel := context.WithCancel(ctx)

	return Consumer{
		closed:          make(chan bool, 1),
		ctx:             ctx,
		cancel:          cancel,
		consumer:        consumer,
		messageReceiver: messageReceiver,
		defaultShutdown: defaultShutdown,
	}
}

// Listen poll the KafkaConsumer for incoming messages and pass onto the Receiver
func (c Consumer) Listen() {
	go func() {
		defer func() {
			// Notify that the consumer loop has closed.
			c.closed <- true
		}()

		logData := log.Data{"package": packageName}
		for {
			select {
			case consumedMessage := <-c.consumer.Channels().Upstream:
				log.Event(c.ctx, "consumer received a message", log.INFO, logData)
				c.messageReceiver.OnMessage(consumedMessage)
				consumedMessage.Commit()
			case <-c.ctx.Done():
				log.Event(c.ctx, "loggercontext done received event, attempting to close consumer", log.INFO, logData)
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
		ctx, cancel = context.WithTimeout(ctx, c.defaultShutdown)
		defer cancel()
	}

	// if the context is not nil but no deadline is set the apply the default
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.defaultShutdown)
		defer cancel()
	}

	// Call cancel to attempt to exit the consumer loop.
	c.cancel()

	logData := log.Data{"package": packageName}

	// Wait for the consumer to tell is has exited or the context timeout occurs.
	select {
	case <-c.closed:
		log.Event(ctx, "gracefully shutdown message consumer", log.INFO, logData)
	case <-ctx.Done():
		log.Event(ctx, "forced shutdown of message consumer", log.INFO, logData)
	}
}
