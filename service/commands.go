package service

var TopicCommands = struct {
	CreateTopic string
	TopicForID  string
}{
	CreateTopic: "CreateTopic",
	TopicForID:  "TopicForID",
}

var ConsumerCommands = struct {
	Connect       string
	Disconnected  string
	ConsumerForID string
}{
	Connect:       "Connect",
	Disconnected:  "Disconnected",
	ConsumerForID: "ConsumerForID",
}

var PartitionsCommands = struct {
	PartitionsForTopic string
	Partitions         string
	PartitionForID     string
	PartitionAdded     string
}{
	PartitionsForTopic: "PartitionsForTopic",
	Partitions:         "Partitions",
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
