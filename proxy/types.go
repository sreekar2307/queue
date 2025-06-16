package main

type lookupResource struct {
	Topic     string
	Partition string
}

type clusterDetails struct {
	LeaderBroker        broker
	BrokersForPartition map[string][]broker
	BrokersForTopic     map[string][]broker
	Brokers             []broker
}

type shardsInfo struct {
	Leader    broker                      `json:"leader"`
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
