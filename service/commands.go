package service

var TopicCommands = struct {
	CreateTopic string
	TopicForID  string
}{
	CreateTopic: "CreateTopic",
	TopicForID:  "TopicForID",
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
}{
	Append: "Append",
}

type Cmd struct {
	CommandType string
	Args        [][]byte
}
