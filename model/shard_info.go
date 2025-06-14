package model

import pbTypes "github.com/sreekar2307/queue/gen/types/v1"

type ShardType string

const (
	ShardTypeBrokers    ShardType = "BROKERS"
	ShardTypePartitions ShardType = "PARTITIONS"
)

type ShardInfo struct {
	ShardType   ShardType
	ShardID     uint64
	Topic       string
	PartitionID string
	Brokers     []*Broker
}

func (s *ShardInfo) ToProtoBuf() *pbTypes.ShardInfo {
	brokers := make([]*pbTypes.Broker, len(s.Brokers))
	for i, broker := range s.Brokers {
		brokers[i] = &pbTypes.Broker{
			Id:               broker.ID,
			RaftAddress:      broker.RaftAddress,
			ReachGrpcAddress: broker.ReachGrpcAddress,
			ReachHttpAddress: broker.ReachHttpAddress,
		}
	}
	shardType := pbTypes.ShardType_SHARD_TYPE_BROKERS
	if s.ShardType == ShardTypePartitions {
		shardType = pbTypes.ShardType_SHARD_TYPE_PARTITIONS
	}
	return &pbTypes.ShardInfo{
		ShardType:   shardType,
		ShardId:     s.ShardID,
		Topic:       s.Topic,
		PartitionId: s.PartitionID,
		Brokers:     brokers,
	}
}

func FromProtoBufShardInfo(pb *pbTypes.ShardInfo) *ShardInfo {
	shardType := ShardTypeBrokers
	if pb.ShardType == pbTypes.ShardType_SHARD_TYPE_PARTITIONS {
		shardType = ShardTypePartitions
	}
	brokers := make([]*Broker, len(pb.Brokers))
	for i, broker := range pb.Brokers {
		brokers[i] = FromProtoBufBroker(broker)
	}
	return &ShardInfo{
		ShardType:   shardType,
		ShardID:     pb.ShardId,
		Topic:       pb.Topic,
		PartitionID: pb.PartitionId,
		Brokers:     brokers,
	}
}
