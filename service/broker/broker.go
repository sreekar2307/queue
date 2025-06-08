package broker

import (
	"context"
	"fmt"

	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/storage"
	"github.com/sreekar2307/queue/util"
)

type DefaultBroker struct {
	MetaDataStorage storage.MetadataStorage
}

func NewDefaultBrokerService(metaDataStorage storage.MetadataStorage) *DefaultBroker {
	return &DefaultBroker{
		MetaDataStorage: metaDataStorage,
	}
}

func (b *DefaultBroker) RegisterBroker(
	ctx context.Context,
	commandID uint64,
	broker *model.Broker,
) (*model.Broker, error) {
	tx, err := b.MetaDataStorage.BeginTransaction(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if err := b.MetaDataStorage.CheckCommandAppliedInTx(ctx, tx, commandID); err != nil {
		return nil, err
	}

	if err := b.MetaDataStorage.CreateBrokerInTx(ctx, tx, broker); err != nil {
		return nil, err
	}

	if err := b.MetaDataStorage.UpdateCommandAppliedInTx(ctx, tx, commandID); err != nil {
		return nil, err
	}

	return broker, tx.Commit()
}

func (b *DefaultBroker) ShardInfoForPartitions(
	ctx context.Context,
	partitions []*model.Partition,
) (map[string]*model.ShardInfo, []*model.Broker, error) {
	brokers, err := b.MetaDataStorage.GetAllBrokers(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get brokers: %w", err)
	}
	brokerForReplicaID := util.GroupBy(brokers, func(broker *model.Broker) uint64 {
		return broker.ID
	})
	shardsInfo := make(map[string]*model.ShardInfo)
	for _, partition := range partitions {
		shardInfo := &model.ShardInfo{
			ShardType:   model.ShardTypePartitions,
			ShardID:     partition.ShardID,
			PartitionID: partition.ID,
			Topic:       partition.TopicName,
			Brokers: util.Map(util.Keys(partition.Members), func(member uint64) *model.Broker {
				return brokerForReplicaID[member][0]
			}),
		}
		shardsInfo[partition.ID] = shardInfo
	}
	return shardsInfo, brokers, nil
}
