package model

type Partition struct {
	ID             string
	TopicName      string
	LeaderBrokerID string
}

const DefaultPartition = "default"

func NewPartition(topicName, partitionID string) *Partition {
	return &Partition{
		TopicName: topicName,
		ID:        partitionID,
	}
}
