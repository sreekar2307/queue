package main

type clusterDetails struct {
	BrokersForPartition map[string][]broker
	BrokersForTopic     map[string][]broker
	Brokers             []broker
}

type shardsInfo struct {
	Brokers   []broker                    `json:"brokers"`
	ShardInfo map[string]partitionDetails `json:"shardInfo"`
}

type partitionDetails struct {
	Brokers     []broker `json:"Brokers"`
	ShardType   string   `json:"ShardType"`
	ShardId     uint64   `json:"ShardId"`
	Topic       string   `json:"Topic"`
	PartitionID string   `json:"PartitionID"`
}

type broker struct {
	Id          uint64 `json:"id"`
	RaftAddress string `json:"RaftAddress"`
	GrpcAddress string `json:"ReachGrpcAddress"`
	HttpAddress string `json:"ReachHttpAddress"`
}
