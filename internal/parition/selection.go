package parition

import "distributedQueue/internal/message"

type PartitionSelectionStrategy interface {
	SelectPartition() string
	AddPartition(string)
	WrittenMessage(string, message.Message)
	ReadMessage(string, string, message.Message)
}
