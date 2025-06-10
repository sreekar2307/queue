package service

var TopicCommands = struct {
	CreateTopic string
	TopicForID  string
}{
	CreateTopic: "CreateTopic",
	TopicForID:  "TopicForID",
}

var BrokerCommands = struct {
	RegisterBroker         string
	ShardInfoForPartitions string
	BrokerForID            string
}{
	RegisterBroker:         "RegisterBroker",
	ShardInfoForPartitions: "ShardInfoForPartitions",
	BrokerForID:            "BrokerForID",
}

var ConsumerCommands = struct {
	Connect        string
	Disconnected   string
	ConsumerForID  string
	Consumers      string
	HealthCheck    string
	UpdateConsumer string
}{
	Connect:        "Connect",
	Disconnected:   "Disconnected",
	ConsumerForID:  "ConsumerForID",
	Consumers:      "Consumers",
	HealthCheck:    "HealthCheck",
	UpdateConsumer: "UpdateConsumer",
}

var PartitionsCommands = struct {
	PartitionsForTopic string
	AllPartitions      string
	PartitionForID     string
	PartitionAdded     string
}{
	PartitionsForTopic: "PartitionsForTopic",
	AllPartitions:      "AllPartitions",
	PartitionForID:     "PartitionID",
	PartitionAdded:     "PartitionAdded",
}

var MessageCommands = struct {
	Append string
	Poll   string
	Ack    string
}{
	Append: "Append",
	Poll:   "Poll",
	Ack:    "Ack",
}

type Cmd struct {
	CommandType string
	Args        [][]byte
}
