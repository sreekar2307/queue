package model

type Partition struct {
	ID        string
	TopicName string
	ShardID   uint64
	Members   map[uint64]string
}

const DefaultPartition = "default"
