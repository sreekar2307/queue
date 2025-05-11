package consumer

import (
	"context"
	stdErrors "errors"
	"fmt"
	"sync"
	"time"

	"github.com/sreekar2307/queue/assignor"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service/errors"
	"github.com/sreekar2307/queue/storage"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"github.com/sreekar2307/queue/util"
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

func (d *DefaultConsumerService) GetConsumer(
	ctx context.Context,
	consumerID string,
) (*model.Consumer, error) {
	return d.MetadataStorage.Consumer(ctx, consumerID)
}

func (d *DefaultConsumerService) UpdateConsumer(
	ctx context.Context,
	commandID uint64,
	consumer *model.Consumer,
) (*model.Consumer, error) {
	if err := d.MetadataStorage.UpdateConsumer(ctx, commandID, consumer); err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return nil, stdErrors.Join(err, errors.ErrDuplicateCommand)
		}
		return nil, fmt.Errorf("failed to update consumer: %w", err)
	}
	return consumer, nil
}

func (d *DefaultConsumerService) AllConsumers(
	ctx context.Context,
) ([]*model.Consumer, error) {
	return d.MetadataStorage.AllConsumers(ctx)
}

func (d *DefaultConsumerService) Connect(
	ctx context.Context,
	commandID uint64,
	consumerGroupID string,
	consumerBrokerID string,
	topicNames []string,
) (*model.Consumer, *model.ConsumerGroup, error) {
	topics, err := d.MetadataStorage.Topics(ctx, topicNames)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get topics: %w", err)
	}
	if len(topics) != len(topicNames) {
		return nil, nil, fmt.Errorf("not all topics exist")
	}
	tx, err := d.MetadataStorage.BeginTransaction(ctx, true)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := d.MetadataStorage.CheckCommandAppliedInTx(ctx, tx, commandID); err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return nil, nil, stdErrors.Join(err, errors.ErrDuplicateCommand)
		}
		return nil, nil, fmt.Errorf("failed to check command applied: %w", err)
	}
	consumerGroup, err := d.MetadataStorage.ConsumerGroupInTx(ctx, tx, consumerGroupID)
	if err != nil {
		if stdErrors.Is(err, storageErrors.ErrConsumerGroupNotFound) {
			consumerGroup = &model.ConsumerGroup{
				ID:     consumerGroupID,
				Topics: util.ToSet(topicNames),
			}
			err = d.MetadataStorage.CreateConsumerGroupInTx(ctx, tx, consumerGroup)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create consumer group: %w", err)
			}
		} else {
			return nil, nil, fmt.Errorf("failed to get consumer group: %w", err)
		}
	}
	connectedConsumer, err := d.MetadataStorage.ConsumerInTx(ctx, tx, consumerBrokerID)
	if err != nil {
		if stdErrors.Is(err, storageErrors.ErrConsumerNotFound) {
			connectedConsumer = &model.Consumer{
				ID:                consumerBrokerID,
				ConsumerGroup:     consumerGroupID,
				LastHealthCheckAt: time.Now().Unix(),
				IsActive:          true,
				Topics:            topicNames,
			}
			err = d.MetadataStorage.CreateConsumerInTx(ctx, tx, connectedConsumer)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create consumer: %w", err)
			}
		} else {
			return nil, nil, fmt.Errorf("failed to get consumer: %w", err)
		}
	} else {
		connectedConsumer.IsActive = true
		connectedConsumer.LastHealthCheckAt = time.Now().Unix()
		connectedConsumer.Topics = topicNames
		err = d.MetadataStorage.UpdateConsumerInTx(ctx, tx, connectedConsumer)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to update consumer: %w", err)
		}
	}
	err = d.MetadataStorage.AddConsumerToGroupInTx(ctx, tx, consumerGroup, connectedConsumer)
	if err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return nil, nil, stdErrors.Join(err, errors.ErrDuplicateCommand)
		}
		return nil, nil, fmt.Errorf("failed to add consumer to group: %w", err)
	}
	if err := d.rebalanceAndUpdateConsumers(ctx, tx, connectedConsumer, consumerGroup); err != nil {
		return nil, nil, fmt.Errorf("failed to rebalance and update consumers: %w", err)
	}
	if err := d.MetadataStorage.UpdateCommandAppliedInTx(ctx, tx, commandID); err != nil {
		return nil, nil, fmt.Errorf("failed to update command applied: %w", err)
	}
	return connectedConsumer, consumerGroup, tx.Commit()
}

func (d *DefaultConsumerService) HealthCheck(
	ctx context.Context,
	commandID uint64,
	consumerID string,
	pingAt int64,
) (*model.Consumer, error) {
	consumer, err := d.MetadataStorage.Consumer(ctx, consumerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer: %w", err)
	}
	consumer.LastHealthCheckAt = pingAt
	if err := d.MetadataStorage.UpdateConsumer(ctx, commandID, consumer); err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return nil, stdErrors.Join(err, errors.ErrDuplicateCommand)
		}
		return nil, fmt.Errorf("failed to update consumer: %w", err)
	}
	return consumer, nil
}

func (d *DefaultConsumerService) Disconnect(
	ctx context.Context,
	commandID uint64,
	consumerBrokerID string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.MetadataStorage.BeginTransaction(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := d.MetadataStorage.CheckCommandAppliedInTx(ctx, tx, commandID); err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return stdErrors.Join(err, errors.ErrDuplicateCommand)
		}
		return fmt.Errorf("failed to check command applied: %w", err)
	}
	disconnectedConsumer, err := d.MetadataStorage.ConsumerInTx(ctx, tx, consumerBrokerID)
	if err != nil {
		return fmt.Errorf("failed to get consumer: %w", err)
	}
	if disconnectedConsumer == nil {
		return fmt.Errorf("consumer not found")
	}
	consumerGroup, err := d.MetadataStorage.ConsumerGroupInTx(ctx, tx, disconnectedConsumer.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("failed to get consumer group: %w", err)
	}
	defer tx.Rollback()
	disconnectedConsumer.IsActive = false
	err = d.MetadataStorage.UpdateConsumerInTx(ctx, tx, disconnectedConsumer)
	if err != nil {
		return fmt.Errorf("failed to update consumer: %w", err)
	}
	err = d.MetadataStorage.RemoveConsumerFromGroupInTx(ctx, tx, consumerGroup, disconnectedConsumer)
	if err != nil {
		return fmt.Errorf("failed to remove consumer from group: %w", err)
	}
	if err := d.rebalanceAndUpdateConsumers(ctx, tx, disconnectedConsumer, consumerGroup); err != nil {
		return fmt.Errorf("failed to rebalance and update consumers: %w", err)
	}
	if err := d.MetadataStorage.UpdateCommandAppliedInTx(ctx, tx, commandID); err != nil {
		return fmt.Errorf("failed to update command applied: %w", err)
	}
	return tx.Commit()
}

func (d *DefaultConsumerService) rebalanceAndUpdateConsumers(
	ctx context.Context,
	tx storage.Transaction,
	currentConsumer *model.Consumer,
	consumerGroup *model.ConsumerGroup,
) error {
	consumerGroup.SetRebalanceInProgress(true)
	if err := d.MetadataStorage.UpdateConsumerGroupInTx(ctx, tx, consumerGroup); err != nil {
		return fmt.Errorf("failed to set rebalance in progress: %w", err)
	}

	prevAssignments, err := d.MetadataStorage.PartitionAssignmentsInTx(ctx, tx, consumerGroup.ID)
	if err != nil {
		return fmt.Errorf("failed to get previous assignments: %w", err)
	}

	partitionsPerConsumer, err := d.PartitionAssignor.Rebalance(ctx, consumerGroup, prevAssignments)
	if err != nil {
		return fmt.Errorf("failed to rebalance partitions: %w", err)
	}
	for consumerID := range consumerGroup.Consumers {
		consumer, err := d.MetadataStorage.ConsumerInTx(ctx, tx, consumerID)
		if err != nil {
			return fmt.Errorf("failed to get consumer %s: %w", consumerID, err)
		}
		consumerPartitions := partitionsPerConsumer[consumerID]
		partitionNames := make([]string, 0, len(consumerPartitions))
		for _, partition := range consumerPartitions {
			partitionNames = append(partitionNames, partition.ID)
		}
		consumer.SetPartitions(partitionNames)
		if err := d.MetadataStorage.UpdateConsumerInTx(ctx, tx, consumer); err != nil {
			return fmt.Errorf("failed to update consumer %s: %w", consumerID, err)
		}
		if consumer.ID == currentConsumer.ID {
			*currentConsumer = *consumer
		}
	}

	consumerGroup.SetRebalanceInProgress(false)
	if err := d.MetadataStorage.UpdateConsumerGroupInTx(ctx, tx, consumerGroup); err != nil {
		return fmt.Errorf("failed to unset rebalance in progress: %w", err)
	}
	return nil
}
