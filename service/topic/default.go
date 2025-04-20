package topic

import (
	"context"
	stdErrors "errors"
	"fmt"
	"hash/crc32"
	"queue/model"
	"queue/storage"
	"queue/storage/errors"
)

type DefaultTopicService struct {
	MetaDataStorage storage.MetadataStorage
}

func NewDefaultTopicService(metaDataStorage storage.MetadataStorage) *DefaultTopicService {
	return &DefaultTopicService{
		MetaDataStorage: metaDataStorage,
	}
}

func (d *DefaultTopicService) CreateTopic(
	ctx context.Context,
	topicName string,
	numPartitions uint64,
) (*model.Topic, error) {
	topic, err := d.MetaDataStorage.Topic(ctx, topicName)
	if err != nil && !stdErrors.Is(err, errors.ErrTopicNotFound) {
		return nil, fmt.Errorf("failed to get topic: %w", err)
	}
	tx, err := d.MetaDataStorage.BeginTransaction(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	topic = &model.Topic{Name: topicName, NumberOfPartitions: numPartitions}
	if err := d.MetaDataStorage.CreateTopicInTx(ctx, tx, topic); err != nil {
		return nil, fmt.Errorf("failed to create topic: %w", err)
	}
	var partitions []*model.Partition
	for i := range int(numPartitions) {
		partition := model.NewPartition(topicName, fmt.Sprintf("%s-%d", topicName, i))
		partitions = append(partitions, partition)
	}
	if err := d.MetaDataStorage.CreatePartitionsInTx(ctx, tx, partitions); err != nil {
		return nil, fmt.Errorf("failed to create partition: %w", err)
	}
	return topic, tx.Commit()
}

func (d *DefaultTopicService) GetTopic(
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

func (d *DefaultTopicService) AllTopics(
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

func (d *DefaultTopicService) GetPartitions(
	ctx context.Context,
	topicName string,
) ([]*model.Partition, error) {
	partitions, err := d.MetaDataStorage.Partitions(ctx, topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions found")
	}
	return partitions, nil
}

func (d *DefaultTopicService) PartitionID(
	ctx context.Context,
	msg *model.Message,
) (string, error) {
	topic, err := d.MetaDataStorage.Topic(ctx, msg.TopicName)
	if err != nil {
		return "", fmt.Errorf("failed to get topic: %w", err)
	}
	var (
		partitionID string
		hash        = crc32.NewIEEE()
	)
	if msg.PartitionKey != "" {
		hash.Write([]byte(msg.PartitionKey))
	} else {
		hash.Write(msg.Data)
	}
	partitionID = fmt.Sprintf("%s-%d", msg.TopicName, uint64(hash.Sum32())%topic.NumberOfPartitions)
	return partitionID, nil
}
