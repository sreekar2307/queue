package selection

import (
	"context"
	"queue/message"
)

type PartitionSelectionStrategy interface {
	SelectPartition(context.Context) string
	AddPartition(context.Context, string)
	WrittenMessage(context.Context, string, *message.Message)
	ReadMessage(context.Context, string, string, *message.Message)
}
