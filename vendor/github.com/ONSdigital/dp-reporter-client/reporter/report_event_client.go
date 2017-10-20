package reporter

import (
	"errors"
	"fmt"

	"github.com/ONSdigital/dp-reporter-client/model"
	"github.com/ONSdigital/dp-reporter-client/schema"
	"github.com/ONSdigital/go-ns/log"
)

const (
	idEmpty          = "cannot Notify, ID is a required field but was empty"
	contextEmpty     = "cannot Notify, errContext is a required field but was empty"
	sendingEvent     = "sending reportEvent for application error"
	failedToMarshal  = "failed to marshal reportEvent to avro"
	eventMessageFMT  = "%s: %s"
	serviceNameEmpty = "cannot create new import error reporter as serviceName is empty"
	kafkaProducerNil = "cannot create new import error reporter as kafkaProducer is nil"
	eventTypeErr     = "error"
	reportEventKey   = "reportEvent"
)

// KafkaProducer interface of the go-ns kafka.Producer
type KafkaProducer interface {
	Output() chan []byte
}

type marshalFunc func(s interface{}) ([]byte, error)

// ErrorReporter is the interface that wraps the Notify method.
// ID and errContent are required parameters. If neither is provided or there is any error while attempting to
// report the event then an error is retuned which the caller can handle as they see fit.
type ErrorReporter interface {
	Notify(id string, errContext string, err error) error
}

// ImportErrorReporter a reporter for sending error reports to the import-reporter
type ImportErrorReporter struct {
	kafkaProducer KafkaProducer
	marshal       marshalFunc
	serviceName   string
}

// NewImportErrorReporterMock create a new ImportErrorReporter to send error reports to the import-reporter
func NewImportErrorReporter(kafkaProducer KafkaProducer, serviceName string) (ImportErrorReporter, error) {
	if kafkaProducer == nil {
		return ImportErrorReporter{}, errors.New(kafkaProducerNil)
	}
	if len(serviceName) == 0 {
		return ImportErrorReporter{}, errors.New(serviceNameEmpty)
	}
	return ImportErrorReporter{
		serviceName:   serviceName,
		kafkaProducer: kafkaProducer,
		marshal:       schema.ReportEventSchema.Marshal,
	}, nil
}

// Notify send an error report to the import-reporter
// ID and errContent are required parameters. If neither is provided or there is any error while attempting to
// report the event then an error is retuned which the caller can handle as they see fit.
func (c ImportErrorReporter) Notify(id string, errContext string, err error) error {
	if len(id) == 0 {
		log.Info(idEmpty, nil)
		return errors.New(idEmpty)
	}
	if len(errContext) == 0 {
		log.Info(contextEmpty, nil)
		return errors.New(contextEmpty)
	}

	reportEvent := &model.ReportEvent{
		InstanceID:  id,
		EventMsg:    fmt.Sprintf(eventMessageFMT, errContext, err.Error()),
		ServiceName: c.serviceName,
		EventType:   eventTypeErr,
	}

	reportEventData := log.Data{reportEventKey: reportEvent}
	log.Info(sendingEvent, reportEventData)

	avroBytes, err := c.marshal(reportEvent)
	if err != nil {
		log.ErrorC(failedToMarshal, err, reportEventData)
		return err
	}

	c.kafkaProducer.Output() <- avroBytes
	return nil
}
