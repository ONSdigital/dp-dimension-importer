package kafka

type Message struct {
	Data []byte
}

func (m *Message) GetData() []byte {
	return m.Data
}
