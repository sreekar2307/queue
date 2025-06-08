package storage

import (
	"context"
	"io"

	"github.com/sreekar2307/queue/model"
)

type MetadataStorage interface {
	Open(context.Context) error
	Close(context.Context) error
	BeginTransaction(ctx context.Context, forWrite bool) (Transaction, error)
	Snapshot(context.Context, io.Writer) error
	RecoverFromSnapshot(context.Context, io.Reader) error

	CreateBrokerInTx(context.Context, Transaction, *model.Broker) error
	GetBrokers(context.Context, map[uint64]bool) ([]*model.Broker, error)
	GetAllBrokers(context.Context) ([]*model.Broker, error)

	CheckCommandAppliedInTx(context.Context, Transaction, uint64) error
	UpdateCommandAppliedInTx(context.Context, Transaction, uint64) error

	CreateTopicInTx(context.Context, Transaction, *model.Topic) error
	Topic(context.Context, string) (*model.Topic, error)
	TopicInTx(context.Context, Transaction, string) (*model.Topic, error)
	Topics(context.Context, []string) ([]*model.Topic, error)
	AllTopics(context.Context) ([]*model.Topic, error)
	CreatePartitionsInTx(context.Context, Transaction, []*model.Partition) error

	Partition(context.Context, string) (*model.Partition, error)
	UpdatePartitionInTx(context.Context, Transaction, *model.Partition) error
	PartitionsForTopic(context.Context, string) ([]*model.Partition, error)
	PartitionsForTopics(context.Context, []string) ([]*model.Partition, error)
	AllPartitions(context.Context) ([]*model.Partition, error)
	LastAppliedCommandID(context.Context) (uint64, error)
	AllPartitionsInTx(context.Context, Transaction) ([]*model.Partition, error)

	CreateConsumerGroupInTx(context.Context, Transaction, *model.ConsumerGroup) error
	ConsumerGroup(context.Context, string) (*model.ConsumerGroup, error)
	PartitionAssignmentsInTx(ctx context.Context, tx Transaction, consumerGroupID string) (map[string][]string, error)
	ConsumerGroupInTx(context.Context, Transaction, string) (*model.ConsumerGroup, error)
	AddConsumerToGroupInTx(context.Context, Transaction, *model.ConsumerGroup, *model.Consumer) error
	UpdateConsumerGroupInTx(context.Context, Transaction, *model.ConsumerGroup) error
	RemoveConsumerFromGroupInTx(context.Context, Transaction, *model.ConsumerGroup, *model.Consumer) error

	UpdateConsumer(context.Context, uint64, *model.Consumer) error
	UpdateConsumerInTx(context.Context, Transaction, *model.Consumer) error
	CreateConsumerInTx(context.Context, Transaction, *model.Consumer) error
	Consumer(context.Context, string) (*model.Consumer, error)
	AllConsumers(context.Context) ([]*model.Consumer, error)
	ConsumerInTx(context.Context, Transaction, string) (*model.Consumer, error)
}
