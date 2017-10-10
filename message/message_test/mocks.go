package message_test

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/go-ns/kafka"
)

type InstanceEventHandler struct {
	HandleFunc func(event.NewInstance) error
}

func (h InstanceEventHandler) Handle(e event.NewInstance) error {
	return h.HandleFunc(e)
}

type MessageReciever struct {
	OnMessageFunc func(message kafka.Message)
}

func (mh MessageReciever) OnMessage(message kafka.Message) {
	mh.OnMessageFunc(message)
}

type MessageMock struct {
	Params    []bool
	Committed chan bool
	Data      []byte
}

func (m *MessageMock) Commit() {
	m.Committed <- true
	m.Params = append(m.Params, true)
}

func (m *MessageMock) GetData() []byte {
	return m.Data
}
