package topic

import (
	"errors"
	"fmt"
	"queue/internal/parition"
	"queue/message"
	"sync"
)

type Topic struct {
	Name                       string
	partitions                 map[string]*parition.Partition
	partitionSelectionStrategy parition.PartitionSelectionStrategy

	mu sync.RWMutex
}

func NewTopic(name string, selectionStrategy parition.PartitionSelectionStrategy) *Topic {
	topic := &Topic{
		Name:                       name,
		partitions:                 make(map[string]*parition.Partition),
		partitionSelectionStrategy: selectionStrategy,
	}
	topic.CreatePartition(parition.DefaultPartition)
	return topic
}

func (t *Topic) CreatePartition(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partitions[key] = parition.NewPartition(key)
	t.partitionSelectionStrategy.AddPartition(key)
}

func (t *Topic) SendMessage(partitionKey string, message *message.Message) (*message.Message, error) {
	t.mu.RLock()
	if _, ok := t.partitions[partitionKey]; !ok {
		t.mu.RUnlock()
		t.CreatePartition(partitionKey)
	} else {
		defer t.mu.RUnlock()
	}
	msg, err := t.partitions[partitionKey].WriteMessage(message)
	if err != nil {
		t.partitionSelectionStrategy.WrittenMessage(partitionKey, msg)
	}
	return msg, err
}

func (t *Topic) ReceiveMessage(consumerGroup string) (*message.Message, error) {
	partitionKey := t.partitionSelectionStrategy.SelectPartition()
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, ok := t.partitions[partitionKey]; !ok {
		return nil, fmt.Errorf("partition '%s' does not exist", partitionKey)
	}
	msg, err := t.partitions[partitionKey].ReadMessage(consumerGroup)
	if errors.Is(err, parition.NoNewMessageErr) {
		return nil, nil
	}
	return msg, err
}

func (t *Topic) AckMessage(msg *message.Message, consumerGroup string) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, ok := t.partitions[msg.PartitionKey()]; !ok {
		return fmt.Errorf("partition '%s' does not exist", msg.PartitionKey)
	}
	err := t.partitions[msg.PartitionKey()].AckMessage(msg, consumerGroup)
	if err != nil {
		t.partitionSelectionStrategy.ReadMessage(msg.PartitionKey(), consumerGroup, msg)
	}
	return err
}
