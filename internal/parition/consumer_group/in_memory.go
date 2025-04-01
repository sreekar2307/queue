package consumer_group

import (
	"context"
	"sync"
)

type inMemoryConsumerGroup struct {
	mu                 sync.RWMutex
	consumerGroupIndex map[string]int
}

func NewInMemoryConsumerGroup() ConsumerGroup {
	return &inMemoryConsumerGroup{
		consumerGroupIndex: make(map[string]int),
	}
}

func (i *inMemoryConsumerGroup) IncrMessageID(_ context.Context, consumerGroup string, latestMessageID int) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.consumerGroupIndex[consumerGroup] = latestMessageID + 1
	return nil
}

func (i *inMemoryConsumerGroup) LatestMessageID(_ context.Context, consumerGroup string) (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.consumerGroupIndex[consumerGroup], nil
}
