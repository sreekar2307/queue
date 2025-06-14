package model

import (
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
	"sync"

	"github.com/lni/dragonboat/v4"
)

type Broker struct {
	ID               uint64
	nh               *dragonboat.NodeHost
	brokerShardId    uint64
	RaftAddress      string
	ReachGrpcAddress string
	ReachHttpAddress string

	mu              sync.RWMutex
	partitionShards map[string]uint64
}

func (b *Broker) ToProtoBuf() *pbTypes.Broker {
	return &pbTypes.Broker{
		Id:               b.ID,
		RaftAddress:      b.RaftAddress,
		ReachGrpcAddress: b.ReachGrpcAddress,
		ReachHttpAddress: b.ReachHttpAddress,
	}
}

func FromProtoBufBroker(pb *pbTypes.Broker) *Broker {
	return &Broker{
		ID:               pb.Id,
		RaftAddress:      pb.RaftAddress,
		ReachGrpcAddress: pb.ReachGrpcAddress,
		ReachHttpAddress: pb.ReachHttpAddress,
	}
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

func (b *Broker) AddShardIDForPartitionID(partitionID string, shardID uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.partitionShards == nil {
		b.partitionShards = make(map[string]uint64)
	}
	b.partitionShards[partitionID] = shardID
}

func (b *Broker) ShardForPartition(partition string) (uint64, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	shardID, ok := b.partitionShards[partition]
	return shardID, ok
}
