package selection

import (
	"context"
	"queue/message"
	"sync"
)

type roundRobinPartitionSelectionStrategy struct {
	currentPartitionIndex int
	partitionsKeys        []string

	mu sync.RWMutex
}

func NewRoundRobinPartitionSelectionStrategy() PartitionSelectionStrategy {
	return &roundRobinPartitionSelectionStrategy{
		currentPartitionIndex: 0,
		partitionsKeys:        make([]string, 0),
	}
}

func (r *roundRobinPartitionSelectionStrategy) SelectPartition(context.Context) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentPartitionIndex >= len(r.partitionsKeys) {
		r.currentPartitionIndex = 0
	}
	partitionKey := r.partitionsKeys[r.currentPartitionIndex]
	r.currentPartitionIndex++
	return partitionKey
}

func (r *roundRobinPartitionSelectionStrategy) AddPartition(_ context.Context, partitionKey string) {
	r.partitionsKeys = append(r.partitionsKeys, partitionKey)
}

func (r *roundRobinPartitionSelectionStrategy) WrittenMessage(context.Context, string, *message.Message) {
	return
}

func (r *roundRobinPartitionSelectionStrategy) ReadMessage(context.Context, string, string, *message.Message) {
	return
}
