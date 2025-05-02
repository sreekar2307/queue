package service

import (
	"context"
	"io"
	"queue/model"
	"queue/service/topic"
)

type (
	TopicService interface {
		CreateTopic(context.Context, uint64, string, uint64, uint64) (*model.Topic, error)
		LastAppliedCommandID(context.Context) (uint64, error)
		GetTopic(context.Context, string) (*model.Topic, error)
		AllTopics(context.Context) ([]*model.Topic, error)
		AllPartitions(context.Context) ([]*model.Partition, error)
		GetPartitions(context.Context, string) ([]*model.Partition, error)
		GetPartition(context.Context, string) (*model.Partition, error)
		PartitionID(context.Context, *model.Message) (string, error)
		UpdatePartition(context.Context, uint64, string, *model.Partition) error
		Snapshot(context.Context, io.Writer) error
		RecoverFromSnapshot(context.Context, io.Reader) error
	}
)

var _ TopicService = (*topic.DefaultTopicService)(nil)
