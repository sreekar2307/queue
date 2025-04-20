package model

type Consumer struct {
	ID            string
	partitions    []string
	ConsumerGroup string

	partitionIndex int
}

func (c *Consumer) IncPartitionIndex() int {
	if len(c.partitions) == 0 {
		panic("no partitions available")
	}
	c.partitionIndex++
	if c.partitionIndex >= len(c.partitions) {
		c.partitionIndex = 0
	}
	return c.partitionIndex
}

func (c *Consumer) PartitionIndex() int {
	if len(c.partitions) == 0 {
		panic("no partitions assigned to consumer")
	}
	if c.partitionIndex >= len(c.partitions) {
		c.partitionIndex = 0
	}
	return c.partitionIndex
}

func (c *Consumer) Partitions() []string {
	return c.partitions
}

func (c *Consumer) SetPartitions(partitions []string) {
	c.partitions = partitions
}
