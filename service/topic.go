package service

import (
	"context"
	"queue/model"
	"queue/service/topic"
)

type (
	TopicService interface {
		CreateTopic(context.Context, string, uint64, uint64) (*model.Topic, error)
		GetTopic(context.Context, string) (*model.Topic, error)
		AllTopics(context.Context) ([]*model.Topic, error)
		AllPartitions(context.Context) ([]*model.Partition, error)
		GetPartitions(context.Context, string) ([]*model.Partition, error)
		PartitionID(context.Context, *model.Message) (string, error)
		UpdatePartition(context.Context, string, *model.Partition) error
	}
)

var _ TopicService = (*topic.DefaultTopicService)(nil)
