package storage

import (
	"context"
	"io"
	"queue/model"
)

type MetadataStorage interface {
	Open(context.Context) error
	Close(context.Context) error
	BeginTransaction(ctx context.Context, forWrite bool) (Transaction, error)
	Snapshot(context.Context, io.Writer) error
	RecoverFromSnapshot(context.Context, io.Reader) error

	CreateTopicInTx(context.Context, Transaction, *model.Topic) error
	Topic(context.Context, string) (*model.Topic, error)
	TopicInTx(context.Context, Transaction, string) (*model.Topic, error)
	Topics(context.Context, []string) ([]*model.Topic, error)
	AllTopics(context.Context) ([]*model.Topic, error)
	CreatePartitionsInTx(context.Context, Transaction, []*model.Partition) error

	Partition(context.Context, string) (*model.Partition, error)
	UpdatePartition(context.Context, *model.Partition) error
	UpdatePartitionInTx(context.Context, Transaction, *model.Partition) error
	Partitions(context.Context, string) ([]*model.Partition, error)
	AllPartitions(context.Context) ([]*model.Partition, error)
	AllPartitionsInTx(context.Context, Transaction) ([]*model.Partition, error)

	CreateConsumerGroup(context.Context, *model.ConsumerGroup) error
	CreateConsumerGroupInTx(context.Context, Transaction, *model.ConsumerGroup) error
	ConsumerGroup(context.Context, string) (*model.ConsumerGroup, error)
	ConsumerGroupInTx(context.Context, Transaction, string) (*model.ConsumerGroup, error)
	AddConsumerToGroupInTx(context.Context, Transaction, *model.ConsumerGroup, *model.Consumer) error
	UpdateConsumerGroup(context.Context, *model.ConsumerGroup) error
	UpdateConsumerGroupInTx(context.Context, Transaction, *model.ConsumerGroup) error
	RemoveConsumerFromGroupInTx(context.Context, Transaction, *model.ConsumerGroup, *model.Consumer) error

	UpdateConsumer(context.Context, *model.Consumer) error
	UpdateConsumerInTx(context.Context, Transaction, *model.Consumer) error
	CreateConsumer(context.Context, *model.Consumer) error
	CreateConsumerInTx(context.Context, Transaction, *model.Consumer) error
	Consumer(context.Context, string) (*model.Consumer, error)
	ConsumerInTx(context.Context, Transaction, string) (*model.Consumer, error)
	DeleteConsumerInTx(context.Context, Transaction, *model.Consumer) error
}
