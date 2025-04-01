package message

import "fmt"

type Message struct {
	messageID    int
	partitionKey string
	data         []byte
}

func NewMessage(data []byte) *Message {
	return &Message{
		data: data,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf("%d, %s, %s", m.messageID, m.partitionKey, m.data)
}

func (m *Message) MessageID() int {
	return m.messageID
}

func (m *Message) SetMessageID(msgID int) {
	m.messageID = msgID
}

func (m *Message) SetData(data []byte) {
	m.data = data
}

func (m *Message) PartitionKey() string {
	return m.partitionKey
}

func (m *Message) SetPartitionKey(partitionKey string) {
	m.partitionKey = partitionKey
}
