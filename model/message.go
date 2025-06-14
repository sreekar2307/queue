package model

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
)

type Message struct {
	Topic        string
	PartitionKey string
	PartitionID  string
	Data         []byte
	ID           []byte
}

func (m *Message) ToProtoBuf() *pbTypes.Message {
	return &pbTypes.Message{
		Topic:        m.Topic,
		PartitionKey: m.PartitionKey,
		PartitionId:  m.PartitionID,
		Data:         m.Data,
		Id:           m.ID,
	}
}

func FromProtoBufMessage(pbMessage *pbTypes.Message) *Message {
	return &Message{
		Topic:        pbMessage.Topic,
		PartitionKey: pbMessage.PartitionKey,
		PartitionID:  pbMessage.PartitionId,
		Data:         pbMessage.Data,
		ID:           pbMessage.Id,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"Message{Topic: %s, PartitionKey: %s, PartitionID: %s, Data: %s, ID: %d}",
		m.Topic, m.PartitionKey, m.PartitionID, base64.StdEncoding.EncodeToString(m.Data),
		binary.BigEndian.Uint64(m.ID),
	)
}
