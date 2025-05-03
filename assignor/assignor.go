package assignor

import (
	"context"
	"queue/assignor/equal"
	"queue/assignor/sticky"
	"queue/model"
)

type PartitionAssignor interface {
	Rebalance(
		ctx context.Context,
		consumerGroup *model.ConsumerGroup,
		prevAssignments map[string][]string,
	) (map[string][]*model.Partition, error)
}

var _ PartitionAssignor = (*sticky.Sticky)(nil)
var _ PartitionAssignor = (*equal.Equal)(nil)
