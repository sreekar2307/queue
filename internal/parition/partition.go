package parition

import (
	"context"
	"queue/internal/parition/consumer_group"
	"queue/message"
	"queue/message/storage"
)

type Partition struct {
	name    string
	storage storage.Storage
	indexer consumer_group.ConsumerGroup
}

func NewPartition(name string) *Partition {
	return &Partition{
		name:    name,
		storage: storage.NewInMemoryStorage(),
		indexer: consumer_group.NewInMemoryConsumerGroup(),
	}
}

func (p *Partition) WriteMessage(ctx context.Context, msg *message.Message) (*message.Message, error) {
	msg.SetPartitionKey(p.name)
	return p.storage.WriteMessage(ctx, msg)
}

func (p *Partition) ReadMessage(ctx context.Context, consumerGroup string) (*message.Message, error) {
	id, err := p.indexer.LatestMessageID(ctx, consumerGroup)
	if err != nil {
		return nil, err
	}
	return p.storage.ReadMessage(ctx, id)
}

func (p *Partition) AckMessage(ctx context.Context, msg *message.Message, consumerGroup string) error {
	return p.indexer.IncrMessageID(ctx, consumerGroup, msg.MessageID())
}
