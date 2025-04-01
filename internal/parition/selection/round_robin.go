package selection

import (
	"distributedQueue/internal/message"
	"distributedQueue/internal/parition"
	"sync"
)

type roundRobinPartitionSelectionStrategy struct {
	currentPartitionIndex int
	partitionsKeys        []string

	mu sync.RWMutex
}

func NewRoundRobinPartitionSelectionStrategy() parition.PartitionSelectionStrategy {
	return &roundRobinPartitionSelectionStrategy{
		currentPartitionIndex: 0,
		partitionsKeys:        make([]string, 0),
	}
}

func (r *roundRobinPartitionSelectionStrategy) SelectPartition() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentPartitionIndex >= len(r.partitionsKeys) {
		r.currentPartitionIndex = 0
	}
	partitionKey := r.partitionsKeys[r.currentPartitionIndex]
	r.currentPartitionIndex++
	return partitionKey
}

func (r *roundRobinPartitionSelectionStrategy) AddPartition(partitionKey string) {
	r.partitionsKeys = append(r.partitionsKeys, partitionKey)
}

func (r *roundRobinPartitionSelectionStrategy) WrittenMessage(string, message.Message) {
	return
}

func (r *roundRobinPartitionSelectionStrategy) ReadMessage(string, string, message.Message) {
	return
}
