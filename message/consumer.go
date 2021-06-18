package message

import (
	"context"

	"github.com/ONSdigital/dp-dimension-importer/config"
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

// Consume spawns a goroutine for each kafka consumer worker, which listens to the Upstream channel and calls the OnMessage on the provided Receiver
// the consumer loops will end when the upstream or closed channels are closed, or when the provided context is Done
func Consume(ctx context.Context, messageConsumer kafka.IConsumerGroup, messageReceiver Receiver, cfg *config.Config) {

	// consume loop, to be executed by each worker
	var consume = func(workerID int) {
		logData := log.Data{"package": packageName, "worker_id": workerID}
		log.Event(ctx, "worker started consuming", logData)
		for {
			select {
			case consumedMessage, ok := <-messageConsumer.Channels().Upstream:
				if !ok {
					log.Event(ctx, "closing event consumer loop because upstream channel is closed", log.INFO, logData)
					return
				}
				messageCtx := context.Background()
				log.Event(messageCtx, "consumer received a message", log.INFO, logData)
				messageReceiver.OnMessage(consumedMessage)
				// The message will always be committed in any case, even if the handling is unsuccessful.
				// This means that the message will not be consumed again in the future.
				consumedMessage.CommitAndRelease()
			case <-ctx.Done():
				log.Event(ctx, "closing event consumer loop because consumer context is Done", log.INFO, logData)
				return
			case <-messageConsumer.Channels().Closer:
				log.Event(ctx, "closing event consumer loop because closer channel is closed", log.INFO, logData)
				return
			}
		}
	}

	// workers to consume messages in parallel
	for w := 1; w <= cfg.KafkaNumWorkers; w++ {
		go consume(w)
	}
}
