package command

type (
	CreateTopicInputs struct {
		TopicName          string `json:"topicName"`
		NumberOfPartitions uint64 `json:"numberOfPartitions"`
		ShardOffset        uint64 `json:"shardOffset"`
	}

	TopicForIDInputs struct {
		TopicName string `json:"topicName"`
	}
)
