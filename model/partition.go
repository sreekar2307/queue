package model

import (
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
)

type Partition struct {
	ID        string
	TopicName string
	ShardID   uint64
	Members   map[uint64]string
}

func (m *Partition) ToProtoBuf() *pbTypes.Partition {
	return &pbTypes.Partition{
		Id:      m.ID,
		Topic:   m.TopicName,
		ShardId: m.ShardID,
		Members: m.Members,
	}
}

func FromProtoBufPartition(pb *pbTypes.Partition) *Partition {
	return &Partition{
		ID:        pb.Id,
		TopicName: pb.Topic,
		ShardID:   pb.ShardId,
		Members:   pb.Members,
	}
}

const DefaultPartition = "default"
