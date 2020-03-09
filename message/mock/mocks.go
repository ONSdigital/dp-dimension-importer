package mock

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
	kafka "github.com/ONSdigital/dp-kafka"
)

// InstanceEventHandler a mocked implementation of message.InstanceEventHandler
// customise the behaviour by setting HandleFunc with your desired functionality
type InstanceEventHandler struct {
	HandleFunc func(event.NewInstance) error
}

// Handle mock implementation of message.InstanceEventHandler.Handle runs the configured func with the supplied event
func (h InstanceEventHandler) Handle(e event.NewInstance) error {
	return h.HandleFunc(e)
}

// MessageReciever a mocked implementation of message.KafkaMessageReciever
// customise the behaviour by providing a OnMessageFunc with your desired functionality
type MessageReciever struct {
	OnMessageFunc func(message kafka.Message)
}

// OnMessage mock implementation of  message.KafkaMessageReciever.OnMessage executes the provided OnMessageFunc with the
// message provided
func (mh MessageReciever) OnMessage(message kafka.Message) {
	mh.OnMessageFunc(message)
}
