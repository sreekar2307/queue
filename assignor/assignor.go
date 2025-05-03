package assignor

import (
	"context"
	"github.com/sreekar2307/queue/assignor/equal"
	"github.com/sreekar2307/queue/assignor/sticky"
	"github.com/sreekar2307/queue/model"
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
