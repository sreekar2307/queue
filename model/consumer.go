package model

import (
	"fmt"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
)

type Consumer struct {
	ID            string
	Partitions    []string
	ConsumerGroup string
	Topics        []string
	IsActive      bool

	partitionIndex    int
	LastHealthCheckAt int64
}

func (c *Consumer) ToProtoBuf() *pbTypes.Consumer {
	return &pbTypes.Consumer{
		Id:            c.ID,
		Partitions:    c.Partitions,
		ConsumerGroup: c.ConsumerGroup,
		Topics:        c.Topics,
		IsActive:      c.IsActive,
	}
}

func FromProtoBufConsumer(pbConsumer *pbTypes.Consumer) *Consumer {
	return &Consumer{
		ID:            pbConsumer.Id,
		Partitions:    pbConsumer.Partitions,
		ConsumerGroup: pbConsumer.ConsumerGroup,
		Topics:        pbConsumer.Topics,
		IsActive:      pbConsumer.IsActive,
	}
}

func (c *Consumer) String() string {
	return fmt.Sprintf("id: %s, partitions: %v, consumerGroup: %s", c.ID, c.Partitions, c.ConsumerGroup)
}

func (c *Consumer) IncPartitionIndex() int {
	if len(c.Partitions) == 0 {
		panic("no partitions available")
	}
	c.partitionIndex++
	if c.partitionIndex >= len(c.Partitions) {
		c.partitionIndex = 0
	}
	return c.partitionIndex
}

func (c *Consumer) PartitionIndex() int {
	if len(c.Partitions) == 0 {
		panic("no partitions assigned to consumer")
	}
	if c.partitionIndex >= len(c.Partitions) {
		c.partitionIndex = 0
	}
	return c.partitionIndex
}

func (c *Consumer) GetCurrentPartition() string {
	if len(c.Partitions) == 0 {
		panic("no partitions assigned to consumer")
	}
	return c.Partitions[c.partitionIndex]
}

func (c *Consumer) SetPartitions(partitions []string) {
	c.Partitions = partitions
}
