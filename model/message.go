package model

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

type Message struct {
	Topic        string
	PartitionKey string
	PartitionID  string
	Data         []byte
	ID           []byte
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"Message{Topic: %s, PartitionKey: %s, PartitionID: %s, Data: %s, ID: %d}",
		m.Topic, m.PartitionKey, m.PartitionID, base64.StdEncoding.EncodeToString(m.Data),
		binary.BigEndian.Uint64(m.ID),
	)
}
