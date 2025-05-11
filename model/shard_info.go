package model

type ShardType string

const (
	ShardTypeBrokers    ShardType = "BROKERS"
	ShardTypePartitions ShardType = "PARTITIONS"
)

type ShardInfo struct {
	ShardType ShardType
	ShardID   uint64
	Brokers   []*Broker
}
