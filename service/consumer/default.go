package consumer

import (
	"context"
	"fmt"
	"queue/assignor"
	"queue/model"
	"queue/storage"
	"queue/util"
	"sync"
)

type DefaultConsumerService struct {
	MetadataStorage   storage.MetadataStorage
	PartitionAssignor assignor.PartitionAssignor

	mu sync.Mutex
}

func NewDefaultConsumerService(
	storage storage.MetadataStorage,
	assignor assignor.PartitionAssignor,
) *DefaultConsumerService {
	return &DefaultConsumerService{
		MetadataStorage:   storage,
		PartitionAssignor: assignor,
	}
}

func (d *DefaultConsumerService) Connect(
	ctx context.Context,
	consumerGroupID string,
	consumerBrokerID string,
	topicNames []string,
) error {
	topics, err := d.MetadataStorage.Topics(ctx, topicNames)
	if err != nil {
		return fmt.Errorf("failed to get topics: %w", err)
	}
	if len(topics) != len(topicNames) {
		return fmt.Errorf("not all topics exist")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.MetadataStorage.BeginTransaction(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	consumerGroup, err := d.MetadataStorage.ConsumerGroupInTx(ctx, tx, consumerGroupID)
	if err != nil {
		return fmt.Errorf("failed to get consumer group: %w", err)
	}
	if consumerGroup == nil {
		err = d.MetadataStorage.CreateConsumerGroupInTx(ctx, tx, &model.ConsumerGroup{
			ID:     consumerGroupID,
			Topics: util.ToSet(topicNames),
		})
		if err != nil {
			return fmt.Errorf("failed to create consumer group: %w", err)
		}
	}
	connectedConsumer, err := d.MetadataStorage.ConsumerInTx(ctx, tx, consumerBrokerID)
	if err != nil {
		return fmt.Errorf("failed to get consumer: %w", err)
	}
	if connectedConsumer == nil {
		connectedConsumer = &model.Consumer{
			ID:            consumerBrokerID,
			ConsumerGroup: consumerGroupID,
		}
		err = d.MetadataStorage.CreateConsumerInTx(ctx, tx, connectedConsumer)
		if err != nil {
			return fmt.Errorf("failed to create consumer: %w", err)
		}
	}
	err = d.MetadataStorage.AddConsumerToGroupInTx(ctx, tx, consumerGroup, connectedConsumer)
	if err != nil {
		return fmt.Errorf("failed to add consumer to group: %w", err)
	}
	if err := d.rebalanceAndUpdateConsumers(ctx, tx, consumerGroup); err != nil {
		return fmt.Errorf("failed to rebalance and update consumers: %w", err)
	}
	return tx.Commit()
}

func (d *DefaultConsumerService) Disconnect(
	ctx context.Context,
	consumerBrokerID string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.MetadataStorage.BeginTransaction(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	disconnectedConsumer, err := d.MetadataStorage.ConsumerInTx(ctx, tx, consumerBrokerID)
	if err != nil {
		return fmt.Errorf("failed to get consumer: %w", err)
	}
	if disconnectedConsumer == nil {
		return fmt.Errorf("consumer not found")
	}
	err = d.MetadataStorage.DeleteConsumerInTx(ctx, tx, disconnectedConsumer)
	if err != nil {
		return fmt.Errorf("failed to delete consumer: %w", err)
	}
	consumerGroup, err := d.MetadataStorage.ConsumerGroupInTx(ctx, tx, disconnectedConsumer.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("failed to get consumer group: %w", err)
	}
	defer tx.Rollback()
	err = d.MetadataStorage.RemoveConsumerFromGroupInTx(ctx, tx, consumerGroup, disconnectedConsumer)
	if err != nil {
		return fmt.Errorf("failed to remove consumer from group: %w", err)
	}
	if err := d.rebalanceAndUpdateConsumers(ctx, tx, consumerGroup); err != nil {
		return fmt.Errorf("failed to rebalance and update consumers: %w", err)
	}
	return tx.Commit()
}

func (d *DefaultConsumerService) rebalanceAndUpdateConsumers(
	ctx context.Context,
	tx storage.Transaction,
	consumerGroup *model.ConsumerGroup,
) error {
	consumerGroup.SetRebalanceInProgress(true)
	if err := d.MetadataStorage.UpdateConsumerGroupInTx(ctx, tx, consumerGroup); err != nil {
		return fmt.Errorf("failed to set rebalance in progress: %w", err)
	}

	partitionsPerConsumer, err := d.PartitionAssignor.Rebalance(ctx, consumerGroup)
	if err != nil {
		return fmt.Errorf("failed to rebalance partitions: %w", err)
	}

	for consumerID, consumerPartitions := range partitionsPerConsumer {
		consumer, err := d.MetadataStorage.ConsumerInTx(ctx, tx, consumerID)
		if err != nil {
			return fmt.Errorf("failed to get consumer %s: %w", consumerID, err)
		}
		partitionNames := make([]string, 0, len(consumerPartitions))
		for _, partition := range consumerPartitions {
			partitionNames = append(partitionNames, partition.ID)
		}
		consumer.SetPartitions(partitionNames)
		if err := d.MetadataStorage.UpdateConsumerInTx(ctx, tx, consumer); err != nil {
			return fmt.Errorf("failed to update consumer %s: %w", consumerID, err)
		}
	}

	consumerGroup.SetRebalanceInProgress(false)
	if err := d.MetadataStorage.UpdateConsumerGroupInTx(ctx, tx, consumerGroup); err != nil {
		return fmt.Errorf("failed to unset rebalance in progress: %w", err)
	}
	return nil
}
