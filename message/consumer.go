package message

import (
	"context"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"time"
)

//go:generate moq -out ./message_test/consumer_generated_mocks.go -pkg message_test . KafkaMessage KafkaConsumer

const (
	eventRecieved        = "recieved NewInstance"
	eventKey             = "event"
	eventHandlerErr      = "unexpected error encountered while handling NewInstance"
	eventHandlerSuccess  = "instance has been successfully imported"
	errorRecieved        = "consumer exit channel recieved error. Exiting dimensionExtractedConsumer"
	conumserErrMsg       = "kafka Consumer Error recieved"
	producerErrMsg       = "completed Error recieved"
	consumerStoppedMsg   = "context.Done invoked. Exiting Consumer loop"
	unmarshallErrMsg     = "unexpected error when unmarshalling avro message to newInstanceEvent"
	processingSuccessful = "instance processed successfully"
	processingErr        = "instance processing failed due to unexpected error kafka message offset will NOT be updated"
)

// KafkaMessage type representing a kafka message.
type KafkaMessage kafka.Message

type KafkaConsumer interface {
	Incoming() chan kafka.Message
}

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
}

// NewConsumer create a NewInstance event consumer.
func NewConsumer(consumer KafkaConsumer, messageReciever Reciever) Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	return Consumer{
		closed:          make(chan bool, 1),
		ctx:             ctx,
		cancel:          cancel,
		consumer:        consumer,
		messageReciever: messageReciever,
	}
}

func (c Consumer) Listen() {
	go func() {
		defer func() {
			// Notify that the consumer loop has closed.
			c.closed <- true
		}()

		for {
			select {
			case consumedMessage := <-c.consumer.Incoming():
				c.messageReciever.OnMessage(consumedMessage)
			case <-c.ctx.Done():
				log.Info(consumerStoppedMsg, nil)
				return
			}
		}
	}()
}

// Close shutdown Consumer.Listen loop.
func (c Consumer) Close(ctx context.Context) {
	// if nil use a default context with a timeout
	if ctx == nil {
		ctx, _ = context.WithTimeout(context.Background(), time.Second*10)
	}

	// if the context is not nil but no deadline is set the apply the default
	if _, ok := ctx.Deadline(); !ok {
		ctx, _ = context.WithTimeout(context.Background(), time.Second*10)
	}

	// Call cancel to attempt to exit the consumer loop.
	c.cancel()

	// Wait for the consumer to tell is has exited or the context timeout occurs.
	select {
	case <-c.closed:
		log.Info("gracefully shutdown consumer loop", nil)
	case <-ctx.Done():
		log.Info("forced shutdown of consumer loop", nil)
	}
}
