package model

import (
	"fmt"
)

type Consumer struct {
	ID            string
	Partitions    []string
	ConsumerGroup string
	IsActive      bool

	partitionIndex    int
	LastHealthCheckAt int64
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
