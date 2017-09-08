package handler

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/dp-dimension-importer/mocks"
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"time"
	"github.com/ONSdigital/dp-dimension-importer/schema"
	"sync"
	"github.com/ONSdigital/go-ns/log"
)

const (
	instance_id      = "0123456789"
	valor_morghullis = "Valar morghulis"
)

var (
	errorEvent = event.ErrorEvent{
		InstanceID: instance_id,
		EventType:  errEventType,
		EventMsg:   valor_morghullis,
	}

	expectedErr = errors.New(valor_morghullis)
)

func TestErrorHandler_Handle(t *testing.T) {
	Convey("Given errorHandler has been configured correctly", t, func() {

		output := make(chan []byte)
		producerMock := &mocks.MessageProducerMock{
			OutputFunc: func() chan []byte {
				return output
			},
		}

		marshallerMock := &mocks.MarshallerMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return schema.ErrorEventSchema.Marshal(s)
			},
		}

		handler := &ErrorHandler{producerMock, marshallerMock}

		Convey("When the handler is given a valid parameters", func() {
			var wg sync.WaitGroup
			var avroBytes []byte

			go func() {
				select {
				case avroBytes = <-output:
					log.Debug("Avro bytes receieved", nil)
					wg.Done()
				case <-time.After(time.Second * 5):
					log.Debug("Test failed due to time out.", nil)
					wg.Done()
				}
			}()

			wg.Add(1)
			handler.Handle(instance_id, expectedErr, nil)
			wg.Wait()

			var actual event.ErrorEvent
			err := schema.ErrorEventSchema.Unmarshal(avroBytes, &actual)

			Convey("Then the output is the expected error event", func() {
				So(actual, ShouldResemble, errorEvent)
				So(err, ShouldBeNil)
			})

			Convey("And Marshaller.Marshal is called 1 time with the expected parameters", func() {
				calls := marshallerMock.MarshalCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].S, ShouldResemble, errorEvent)
			})

			Convey("And Producer.Output is called 1 time with the expected parameters", func() {
				calls := producerMock.OutputCalls()
				So(len(calls), ShouldEqual, 1)
			})

		})
	})
}

func TestErrorHandler_Handle_MarshallerError(t *testing.T) {
	Convey("Given eventHandler has been configured correctly", t, func() {
		expectedErr := errors.New(valor_morghullis)
		output := make(chan []byte)
		producerMock := &mocks.MessageProducerMock{
			OutputFunc: func() chan []byte {
				return output
			},
		}

		marshallerMock := &mocks.MarshallerMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return nil, expectedErr
			},
		}

		handler := &ErrorHandler{producerMock, marshallerMock}

		Convey("When marshaller.Marshal returns an error", func() {
			handler.Handle(instance_id, expectedErr, nil)

			Convey("And Marshaller.Marshal is called 1 time with the expected parameters", func() {
				calls := marshallerMock.MarshalCalls()
				So(len(calls), ShouldEqual, 1)
				So(calls[0].S, ShouldResemble, errorEvent)
			})

			Convey("And Producer.Output is never called.", func() {
				calls := producerMock.OutputCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})
}
