package message_test

import (
	"github.com/ONSdigital/dp-dimension-importer/event"
)

type EventHandlerMock struct {
	Param           []event.DimensionsExtractedEvent
	HandleEventFunc func(event.DimensionsExtractedEvent) error
}

func (m *EventHandlerMock) HandleEvent(event event.DimensionsExtractedEvent) error {
	m.Param = append(m.Param, event)
	return m.HandleEventFunc(event)
}
