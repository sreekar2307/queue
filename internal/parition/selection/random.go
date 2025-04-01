package selection

import (
	"context"
	"math/rand/v2"
	"queue/message"
	"sync"
)

type randomSelectionStrategy struct {
	partitionsKeys []string

	mu sync.RWMutex
}

func NewRandomSelectionStrategy() PartitionSelectionStrategy {
	return &randomSelectionStrategy{
		partitionsKeys: make([]string, 0),
	}
}

func (r *randomSelectionStrategy) SelectPartition(context.Context) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.partitionsKeys[rand.IntN(len(r.partitionsKeys))]
}

func (r *randomSelectionStrategy) AddPartition(_ context.Context, partitionKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitionsKeys = append(r.partitionsKeys, partitionKey)
}

func (r *randomSelectionStrategy) WrittenMessage(context.Context, string, *message.Message) {
	return
}

func (r *randomSelectionStrategy) ReadMessage(context.Context, string, string, *message.Message) {
	return
}
