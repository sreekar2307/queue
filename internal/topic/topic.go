package topic

import (
	"context"
	"fmt"
	"queue/internal/parition"
	"queue/internal/parition/selection"
	"queue/message"
	"sync"
)

type Topic struct {
	Name                       string
	partitions                 map[string]*parition.Partition
	partitionSelectionStrategy selection.PartitionSelectionStrategy

	mu sync.RWMutex
}

func NewTopic(ctx context.Context, name string, selectionStrategy selection.PartitionSelectionStrategy) *Topic {
	topic := &Topic{
		Name:                       name,
		partitions:                 make(map[string]*parition.Partition),
		partitionSelectionStrategy: selectionStrategy,
	}
	topic.CreatePartition(ctx, parition.DefaultPartition)
	return topic
}

func (t *Topic) CreatePartition(ctx context.Context, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partitions[key] = parition.NewPartition(key)
	t.partitionSelectionStrategy.AddPartition(ctx, key)
}

func (t *Topic) SendMessage(ctx context.Context, partitionKey string, message *message.Message) (*message.Message, error) {
	t.mu.RLock()
	if _, ok := t.partitions[partitionKey]; !ok {
		t.mu.RUnlock()
		t.CreatePartition(ctx, partitionKey)
	} else {
		defer t.mu.RUnlock()
	}
	msg, err := t.partitions[partitionKey].WriteMessage(ctx, message)
	if err != nil {
		t.partitionSelectionStrategy.WrittenMessage(ctx, partitionKey, msg)
	}
	return msg, err
}

func (t *Topic) ReceiveMessage(ctx context.Context, consumerGroup string) (*message.Message, error) {
	partitionKey := t.partitionSelectionStrategy.SelectPartition(ctx)
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, ok := t.partitions[partitionKey]; !ok {
		return nil, fmt.Errorf("partition '%s' does not exist", partitionKey)
	}
	msg, err := t.partitions[partitionKey].ReadMessage(ctx, consumerGroup)
	return msg, err
}

func (t *Topic) AckMessage(ctx context.Context, msg *message.Message, consumerGroup string) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, ok := t.partitions[msg.PartitionKey()]; !ok {
		return fmt.Errorf("partition '%s' does not exist", msg.PartitionKey)
	}
	err := t.partitions[msg.PartitionKey()].AckMessage(ctx, msg, consumerGroup)
	if err != nil {
		t.partitionSelectionStrategy.ReadMessage(ctx, msg.PartitionKey(), consumerGroup, msg)
	}
	return err
}
