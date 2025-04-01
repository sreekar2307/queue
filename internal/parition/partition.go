package parition

import (
	"context"
	"fmt"
	"queue/internal/parition/storage"
	"queue/message"
	"sync"
)

type Partition struct {
	name               string
	messages           []*message.Message
	consumerGroupIndex map[string]int
	storage            storage.Storage

	messagesMu      sync.RWMutex
	consumerGroupMu sync.RWMutex
}

var NoNewMessageErr = fmt.Errorf("no new messages")

func NewPartition(name string) *Partition {
	return &Partition{
		name:               name,
		messages:           make([]*message.Message, 0),
		consumerGroupIndex: make(map[string]int),
		storage:            storage.NewInMemoryStorage(),
	}
}

func (p *Partition) WriteMessage(ctx context.Context, msg *message.Message) (*message.Message, error) {
	msg.SetPartitionKey(p.name)
	return p.storage.WriteMessage(ctx, msg)
}

func (p *Partition) ReadMessage(ctx context.Context, consumerGroup string) (*message.Message, error) {
	p.consumerGroupMu.RLock()
	id := p.consumerGroupIndex[consumerGroup]
	p.consumerGroupMu.RUnlock()
	return p.storage.ReadMessage(ctx, id)
}

func (p *Partition) AckMessage(ctx context.Context, msg *message.Message, consumerGroup string) error {
	p.consumerGroupMu.Lock()
	defer p.consumerGroupMu.Unlock()
	p.consumerGroupIndex[consumerGroup] = msg.MessageID() + 1
	return nil
}
