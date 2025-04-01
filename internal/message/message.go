package message

import "fmt"

type Message struct {
	MessageID    int
	PartitionKey string
	Data         []byte
}

func (m Message) String() string {
	return fmt.Sprintf("%d, %s, %s", m.MessageID, m.PartitionKey, m.Data)
}
