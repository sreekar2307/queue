package equal

import (
	"cmp"
	"context"
	"fmt"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/storage"
	"github.com/sreekar2307/queue/util"
	"slices"
)

type Equal struct {
	metadata storage.MetadataStorage
}

func NewAssignor(metadata storage.MetadataStorage) *Equal {
	return &Equal{
		metadata: metadata,
	}
}

func (e *Equal) Rebalance(
	ctx context.Context,
	consumerGroup *model.ConsumerGroup,
	_ map[string][]string,
) (map[string][]*model.Partition, error) {
	partitions, err := e.metadata.PartitionsForTopics(ctx, util.Keys(consumerGroup.Topics))
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	slices.SortFunc(partitions, func(i, j *model.Partition) int {
		return cmp.Compare(i.ID, j.ID)
	})
	consumers := util.Keys(consumerGroup.Consumers)
	slices.Sort(consumers)
	partitionDivision := make(map[string][]*model.Partition, len(consumers))
	for i, partition := range partitions {
		consumer := consumers[i%len(consumers)]
		partitionDivision[consumer] = append(partitionDivision[consumer], partition)
	}
	return partitionDivision, nil
}
