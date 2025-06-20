package topic

import (
	"context"
	stdErrors "errors"
	"fmt"
	"github.com/sreekar2307/queue/logger"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service"
	"github.com/sreekar2307/queue/storage"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"hash/crc32"
	"io"
)

type topicService struct {
	MetaDataStorage storage.MetadataStorage
	log             logger.Logger
}

func NewTopicService(metaDataStorage storage.MetadataStorage, log logger.Logger) service.TopicService {
	return &topicService{
		MetaDataStorage: metaDataStorage,
		log:             log,
	}
}

func (d *topicService) CreateTopic(
	ctx context.Context,
	commandID uint64,
	topicName string,
	numPartitions uint64,
	offsetSharID uint64,
) (*model.Topic, error) {
	tx, err := d.MetaDataStorage.BeginTransaction(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := d.MetaDataStorage.CheckCommandAppliedInTx(ctx, tx, commandID); err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return nil, stdErrors.Join(err, storageErrors.ErrDuplicateCommand)
		}
		return nil, fmt.Errorf("failed to check command applied: %w", err)
	}
	topic, err := d.MetaDataStorage.TopicInTx(ctx, tx, topicName)
	if err != nil {
		if stdErrors.Is(err, storageErrors.ErrTopicNotFound) {
			topic = &model.Topic{Name: topicName, NumberOfPartitions: numPartitions}
			err = d.MetaDataStorage.CreateTopicInTx(ctx, tx, topic)
			if err != nil {
				return nil, fmt.Errorf("failed to create topic: %w", err)
			}
		}
	} else {
		return nil, storageErrors.ErrTopicAlreadyExists
	}
	allPartitions, err := d.MetaDataStorage.AllPartitionsInTx(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all partitions: %w", err)
	}
	var partitions []*model.Partition
	for i := range int(numPartitions) {
		partition := &model.Partition{
			ID:        fmt.Sprintf("%s-%d", topicName, i),
			TopicName: topicName,
			ShardID:   uint64(len(allPartitions) + int(offsetSharID) + i),
		}
		partitions = append(partitions, partition)
	}
	if err := d.MetaDataStorage.CreatePartitionsInTx(ctx, tx, partitions); err != nil {
		return nil, fmt.Errorf("failed to create partition: %w", err)
	}
	if err := d.MetaDataStorage.UpdateCommandAppliedInTx(ctx, tx, commandID); err != nil {
		return nil, fmt.Errorf("failed to update command applied: %w", err)
	}
	return topic, tx.Commit()
}

func (d *topicService) LastAppliedCommandID(ctx context.Context, _ uint64) (uint64, error) {
	// as metadata is maintained in a single shard, we can ignore shardID
	lastAppliedCommandID, err := d.MetaDataStorage.LastAppliedCommandID(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get last applied command ID: %w", err)
	}
	return lastAppliedCommandID, nil
}

func (d *topicService) GetTopic(
	ctx context.Context,
	topicName string,
) (*model.Topic, error) {
	topic, err := d.MetaDataStorage.Topic(ctx, topicName)
	if err != nil {
		return topic, fmt.Errorf("failed to get topic: %w", err)
	}
	if topic != nil {
		return topic, fmt.Errorf("topic not found")
	}
	return topic, nil
}

func (d *topicService) AllTopics(
	ctx context.Context,
) ([]*model.Topic, error) {
	topics, err := d.MetaDataStorage.AllTopics(ctx)
	if err != nil {
		return topics, fmt.Errorf("failed to get topics: %w", err)
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("no topics found")
	}
	return topics, nil
}

func (d *topicService) AllPartitions(
	ctx context.Context,
) ([]*model.Partition, error) {
	partitions, err := d.MetaDataStorage.AllPartitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	return partitions, nil
}

func (d *topicService) GetPartitions(
	ctx context.Context,
	topicName string,
) ([]*model.Partition, error) {
	partitions, err := d.MetaDataStorage.PartitionsForTopic(ctx, topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions found")
	}
	return partitions, nil
}

func (d *topicService) GetPartition(
	ctx context.Context,
	partitionID string,
) (*model.Partition, error) {
	return d.MetaDataStorage.Partition(ctx, partitionID)
}

func (d *topicService) PartitionID(
	ctx context.Context,
	msg *model.Message,
) (string, error) {
	topic, err := d.MetaDataStorage.Topic(ctx, msg.Topic)
	if err != nil {
		return "", fmt.Errorf("failed to get topic: %w", err)
	}
	var (
		partitionID string
		hash        = crc32.NewIEEE()
	)
	if len(msg.PartitionKey) != 0 {
		hash.Write([]byte(msg.PartitionKey))
	} else {
		hash.Write(msg.Data)
	}
	partitionID = fmt.Sprintf("%s-%d", msg.Topic, uint64(hash.Sum32())%topic.NumberOfPartitions)
	return partitionID, nil
}

func (d *topicService) UpdatePartition(
	ctx context.Context,
	commandID uint64,
	partitionID string,
	partitionUpdates *model.Partition,
) error {
	if partitionUpdates == nil {
		return fmt.Errorf("partition details are nil")
	}
	tx, err := d.MetaDataStorage.BeginTransaction(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := d.MetaDataStorage.CheckCommandAppliedInTx(ctx, tx, commandID); err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return stdErrors.Join(err, storageErrors.ErrDuplicateCommand)
		}
		return fmt.Errorf("failed to check command applied: %w", err)
	}
	partition, err := d.MetaDataStorage.Partition(ctx, partitionID)
	if err != nil {
		return fmt.Errorf("failed to get partition: %w", err)
	}
	partition.Members = partitionUpdates.Members
	partition.ShardID = partitionUpdates.ShardID
	if err := d.MetaDataStorage.UpdatePartitionInTx(ctx, tx, partition); err != nil {
		return fmt.Errorf("failed to update partition: %w", err)
	}
	if err := d.MetaDataStorage.UpdateCommandAppliedInTx(ctx, tx, commandID); err != nil {
		return fmt.Errorf("failed to update command applied: %w", err)
	}
	*partitionUpdates = *partition
	return tx.Commit()
}

func (d *topicService) Snapshot(ctx context.Context, writer io.Writer) error {
	if err := d.MetaDataStorage.Snapshot(ctx, writer); err != nil {
		return fmt.Errorf("failed to snapshot message storage: %w", err)
	}
	return nil
}

func (d *topicService) RecoverFromSnapshot(ctx context.Context, reader io.Reader) error {
	if err := d.MetaDataStorage.RecoverFromSnapshot(ctx, reader); err != nil {
		return fmt.Errorf("failed to recover message storage: %w", err)
	}
	return nil
}
