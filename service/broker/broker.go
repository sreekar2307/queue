package broker

import (
	"context"
	"fmt"
	"github.com/sreekar2307/queue/service"

	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/storage"
	"github.com/sreekar2307/queue/util"
)

type broker struct {
	MetaDataStorage storage.MetadataStorage
}

func NewBrokerService(metaDataStorage storage.MetadataStorage) service.BrokerService {
	return &broker{
		MetaDataStorage: metaDataStorage,
	}
}

func (b *broker) RegisterBroker(
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

func (b *broker) GetBroker(
	ctx context.Context,
	brokerID uint64,
) (*model.Broker, error) {
	broker, err := b.MetaDataStorage.GetBroker(ctx, brokerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get broker %d: %w", brokerID, err)
	}
	if broker == nil {
		return nil, fmt.Errorf("broker %d not found", brokerID)
	}
	return broker, nil
}

func (b *broker) ShardInfoForPartitions(
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
