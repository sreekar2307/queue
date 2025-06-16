package assignor

import (
	"context"

	"github.com/sreekar2307/queue/model"
)

type PartitionAssignor interface {
	Rebalance(
		ctx context.Context,
		consumerGroup *model.ConsumerGroup,
		prevAssignments map[string][]string,
	) (map[string][]*model.Partition, error)
}
