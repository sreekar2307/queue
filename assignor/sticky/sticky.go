package sticky

import (
	"cmp"
	"context"
	"fmt"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/storage"
	"github.com/sreekar2307/queue/util"
	"slices"
)

type Sticky struct {
	metadata storage.MetadataStorage
}

func NewAssignor(metadata storage.MetadataStorage) *Sticky {
	return &Sticky{
		metadata: metadata,
	}
}

func (s *Sticky) Rebalance(
	ctx context.Context,
	consumerGroup *model.ConsumerGroup,
	prevAssignments map[string][]string,
) (map[string][]*model.Partition, error) {
	partitions, err := s.metadata.PartitionsForTopics(ctx, util.Keys(consumerGroup.Topics))
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	slices.SortFunc(partitions, func(i, j *model.Partition) int {
		return cmp.Compare(i.ID, j.ID)
	})

	consumers := util.Keys(consumerGroup.Consumers)
	slices.Sort(consumers)

	assignments := make(map[string][]*model.Partition, len(consumers))
	assignedPartitions := make(map[string]bool)

	partitionMap := make(map[string]*model.Partition, len(partitions))
	for _, p := range partitions {
		partitionMap[p.ID] = p
	}

	for consumerID, partitionIDs := range prevAssignments {
		if _, ok := consumerGroup.Consumers[consumerID]; !ok {
			continue
		}
		for _, pid := range partitionIDs {
			if p, exists := partitionMap[pid]; exists && !assignedPartitions[pid] {
				assignments[consumerID] = append(assignments[consumerID], p)
				assignedPartitions[pid] = true
			}
		}
	}

	for _, p := range partitions {
		if assignedPartitions[p.ID] {
			continue
		}

		var targetConsumer string
		minCount := int(^uint(0) >> 1) // Max int
		for _, consumer := range consumers {
			count := len(assignments[consumer])
			if count < minCount {
				minCount = count
				targetConsumer = consumer
			}
		}

		assignments[targetConsumer] = append(assignments[targetConsumer], p)
		assignedPartitions[p.ID] = true
	}

	return assignments, nil
}
