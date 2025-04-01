package parition

import (
	"queue/message"
)

type PartitionSelectionStrategy interface {
	SelectPartition() string
	AddPartition(string)
	WrittenMessage(string, *message.Message)
	ReadMessage(string, string, *message.Message)
}
