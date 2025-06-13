package command

var TopicCommands = struct {
	CreateTopic Kind
	TopicForID  Kind
}{
	CreateTopic: "CreateTopic",
	TopicForID:  "TopicForID",
}

var BrokerCommands = struct {
	RegisterBroker         Kind
	ShardInfoForPartitions Kind
	BrokerForID            Kind
}{
	RegisterBroker:         "RegisterBroker",
	ShardInfoForPartitions: "ShardInfoForPartitions",
	BrokerForID:            "BrokerForID",
}

var ConsumerCommands = struct {
	Connect        Kind
	Disconnected   Kind
	ConsumerForID  Kind
	Consumers      Kind
	HealthCheck    Kind
	UpdateConsumer Kind
}{
	Connect:        "Connect",
	Disconnected:   "Disconnected",
	ConsumerForID:  "ConsumerForID",
	Consumers:      "Consumers",
	HealthCheck:    "HealthCheck",
	UpdateConsumer: "UpdateConsumer",
}

var PartitionsCommands = struct {
	PartitionsForTopic Kind
	AllPartitions      Kind
	PartitionForID     Kind
	PartitionAdded     Kind
}{
	PartitionsForTopic: "PartitionsForTopic",
	AllPartitions:      "AllPartitions",
	PartitionForID:     "PartitionID",
	PartitionAdded:     "PartitionAdded",
}

var MessageCommands = struct {
	Append Kind
	Poll   Kind
	Ack    Kind
}{
	Append: "Append",
	Poll:   "Poll",
	Ack:    "Ack",
}
