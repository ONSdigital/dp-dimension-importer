package message

import (
	"context"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/v2/log"
)

//go:generate moq -out mock/receiver.go -pkg mock . Receiver

var packageName = "handler.InstanceEventHandler"

// KafkaMessage type representing a kafka message.
type KafkaMessage kafka.Message

// Receiver is sent a kafka messages and processes it
type Receiver interface {
	OnMessage(message kafka.Message)
}

// Consume spawns a goroutine for each kafka consumer worker, which listens to the Upstream channel and calls the OnMessage on the provided Receiver
// the consumer loops will end when the upstream or closed channels are closed, or when the provided context is Done
func Consume(ctx context.Context, messageConsumer kafka.IConsumerGroup, messageReceiver Receiver, kafkaNumWorkers int) {
	// consume loop, to be executed by each worker
	var consume = func(workerID int) {
		logData := log.Data{"package": packageName, "worker_id": workerID}
		log.Info(ctx, "worker started consuming", logData)
		for {
			select {
			case consumedMessage, ok := <-messageConsumer.Channels().Upstream:
				if !ok {
					log.Info(ctx, "closing event consumer loop because upstream channel is closed", logData)
					return
				}
				messageCtx := context.Background()
				log.Info(messageCtx, "consumer received a message", logData)
				messageReceiver.OnMessage(consumedMessage)
				// The message will always be committed in any case, even if the handling is unsuccessful.
				// This means that the message will not be consumed again in the future.
				consumedMessage.CommitAndRelease()
			case <-ctx.Done():
				log.Info(ctx, "closing event consumer loop because consumer context is Done", logData)
				return
			case <-messageConsumer.Channels().Closer:
				log.Info(ctx, "closing event consumer loop because closer channel is closed", logData)
				return
			}
		}
	}

	// workers to consume messages in parallel
	for w := 1; w <= kafkaNumWorkers; w++ {
		go consume(w)
	}
}
