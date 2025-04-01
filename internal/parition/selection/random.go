package selection

import (
	"queue/internal/message"
	"queue/internal/parition"
	"math/rand/v2"
	"sync"
)

type randomSelectionStrategy struct {
	partitionsKeys []string

	mu sync.RWMutex
}

func NewRandomSelectionStrategy() parition.PartitionSelectionStrategy {
	return &randomSelectionStrategy{
		partitionsKeys: make([]string, 0),
	}
}

func (r *randomSelectionStrategy) SelectPartition() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.partitionsKeys[rand.IntN(len(r.partitionsKeys))]
}

func (r *randomSelectionStrategy) AddPartition(partitionKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitionsKeys = append(r.partitionsKeys, partitionKey)
}

func (r *randomSelectionStrategy) WrittenMessage(partitionKey string, msg message.Message) {
	return
}

func (r *randomSelectionStrategy) ReadMessage(string, string, message.Message) {
	return
}
