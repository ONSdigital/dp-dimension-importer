package reportertest

// KafkaProducerMock a mock of the go-ns kafka.Producer
type KafkaProducerMock struct {
	output      chan []byte
	outputCalls int
}

// NewKafkaProducerMock convenience func for create a new KafkaProducerMock for your tests
func NewKafkaProducerMock(output chan []byte) *KafkaProducerMock {
	return &KafkaProducerMock{
		output:      output,
		outputCalls: 0,
	}
}

func (m *KafkaProducerMock) Output() chan []byte {
	m.outputCalls += 1
	return m.output
}

// OutputCalls return the number times Output() was invoked on this mock
func (m *KafkaProducerMock) OutputCalls() int {
	return m.outputCalls
}
