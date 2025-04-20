package service

var TopicCommands = struct {
	Create string
	Get    string
}{
	Create: "CreateTopic",
	Get:    "GetTopic",
}

var PartitionsCommands = struct {
	GetPartitions  string
	PartitionID    string
	PartitionAdded string
}{
	GetPartitions:  "GetPartitions",
	PartitionID:    "PartitionID",
	PartitionAdded: "PartitionAdded",
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
