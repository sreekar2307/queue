package model

import (
	"fmt"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
)

type Topic struct {
	Name               string
	NumberOfPartitions uint64
}

func (t *Topic) String() string {
	return fmt.Sprintf("Topic{Name: %s, NumberOfPartitions: %d}", t.Name, t.NumberOfPartitions)
}

func (t *Topic) ToProtoBuf() *pbTypes.Topic {
	return &pbTypes.Topic{
		Topic:           t.Name,
		NumOfPartitions: t.NumberOfPartitions,
	}
}

func FromProtoBufTopic(pb *pbTypes.Topic) *Topic {
	return &Topic{
		Name:               pb.Topic,
		NumberOfPartitions: pb.NumOfPartitions,
	}
}
