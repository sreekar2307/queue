package model

import (
	"sync"

	"github.com/lni/dragonboat/v4"
)

type Broker struct {
	ID            uint64
	nh            *dragonboat.NodeHost
	brokerShardId uint64

	mu              sync.RWMutex
	partitionShards map[string]uint64
}

func (b *Broker) BrokerShardId() uint64 {
	return b.brokerShardId
}

func (b *Broker) SetBrokerShardId(brokerShardId uint64) {
	b.brokerShardId = brokerShardId
}

func (b *Broker) SetNodeHost(nh *dragonboat.NodeHost) {
	b.nh = nh
}

func (b *Broker) NodeHost() *dragonboat.NodeHost {
	return b.nh
}

func (b *Broker) AddPartitionShards(partition string, shardID uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.partitionShards == nil {
		b.partitionShards = make(map[string]uint64)
	}
	b.partitionShards[partition] = shardID
}

func (b *Broker) ShardForPartition(partition string) (uint64, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	shardID, ok := b.partitionShards[partition]
	return shardID, ok
}
