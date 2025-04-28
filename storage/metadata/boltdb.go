package metadata

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"queue/model"
	"queue/storage"
	"queue/storage/errors"
	"queue/util"

	boltDB "go.etcd.io/bbolt"
)

type Bolt struct {
	db     *boltDB.DB
	dbPath string
}

func NewBolt(dbPath string) *Bolt {
	return &Bolt{
		dbPath: dbPath,
	}
}

const (
	topicsBucket         = "topics"
	consumersBucket      = "consumers"
	consumerGroupsBucket = "consumer_groups"
	partitionsBucket     = "partitions"
)

func (m *Bolt) Open(_ context.Context) error {
	newDB, err := boltDB.Open(m.dbPath, 0777, nil)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	m.db = newDB
	return nil
}

func (m *Bolt) Close(_ context.Context) error {
	if err := m.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	return nil
}

func (m *Bolt) BeginTransaction(_ context.Context, forWrite bool) (storage.Transaction, error) {
	tx, err := m.db.Begin(forWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &storage.BoltDbTransactionWrapper{BoltTx: tx}, nil
}

func (m *Bolt) CreateTopicInTx(_ context.Context, tx storage.Transaction, topic *model.Topic) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(topicsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	topicData, err := json.Marshal(topic)
	if err != nil {
		return fmt.Errorf("failed to marshal topic: %w", err)
	}
	if err := bucket.Put([]byte(topic.Name), topicData); err != nil {
		return fmt.Errorf("failed to put topic: %w", err)
	}
	return nil
}

func (m *Bolt) Topic(ctx context.Context, s string) (*model.Topic, error) {
	var topic model.Topic
	err := m.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(topicsBucket))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(s))
		if data == nil {
			return nil
		}
		if err := json.Unmarshal(data, &topic); err != nil {
			return fmt.Errorf("failed to unmarshal topic: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get topic: %w", err)
	}
	if len(topic.Name) == 0 {
		return nil, errors.ErrTopicNotFound
	}
	partitions, err := m.PartitionsForTopic(ctx, topic.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	topic.NumberOfPartitions = uint64(len(partitions))
	return &topic, nil
}

func (m *Bolt) TopicInTx(ctx context.Context, tx storage.Transaction, s string) (*model.Topic, error) {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(topicsBucket))
	if bucket == nil {
		return nil, errors.ErrTopicNotFound
	}
	data := bucket.Get([]byte(s))
	if data == nil {
		return nil, errors.ErrTopicNotFound
	}
	var topic model.Topic
	if err := json.Unmarshal(data, &topic); err != nil {
		return nil, fmt.Errorf("failed to unmarshal topic: %w", err)
	}
	partitions, err := m.PartitionsInTx(ctx, tx, map[string]bool{topic.Name: true})
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	topic.NumberOfPartitions = uint64(len(partitions))
	return &topic, nil
}

func (m *Bolt) Topics(ctx context.Context, topicNames []string) ([]*model.Topic, error) {
	var topics []*model.Topic
	err := m.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(topicsBucket))
		if bucket == nil {
			return nil
		}
		for _, s := range topicNames {
			data := bucket.Get([]byte(s))
			if data == nil {
				continue
			}
			var topic model.Topic
			if err := json.Unmarshal(data, &topic); err != nil {
				return fmt.Errorf("failed to unmarshal topic: %w", err)
			}
			partitions, err := m.PartitionsForTopic(ctx, topic.Name)
			if err != nil {
				return fmt.Errorf("failed to get partitions: %w", err)
			}
			topic.NumberOfPartitions = uint64(len(partitions))
			topics = append(topics, &topic)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get topics: %w", err)
	}
	return topics, nil
}

func (m *Bolt) AllTopics(ctx context.Context) ([]*model.Topic, error) {
	var topics []*model.Topic
	err := m.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(topicsBucket))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var topic model.Topic
			if err := json.Unmarshal(v, &topic); err != nil {
				return fmt.Errorf("failed to unmarshal topic: %w", err)
			}
			partitions, err := m.PartitionsForTopic(ctx, topic.Name)
			if err != nil {
				return fmt.Errorf("failed to get partitions: %w", err)
			}
			topic.NumberOfPartitions = uint64(len(partitions))
			topics = append(topics, &topic)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get topics: %w", err)
	}
	return topics, nil
}

func (m *Bolt) CreatePartitionsInTx(
	_ context.Context,
	transaction storage.Transaction,
	partitions []*model.Partition,
) error {
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(partitionsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	for _, partition := range partitions {
		partitionData, err := json.Marshal(partition)
		if err != nil {
			return fmt.Errorf("failed to marshal partition: %w", err)
		}
		if err := bucket.Put([]byte(partition.ID), partitionData); err != nil {
			return fmt.Errorf("failed to put partition: %w", err)
		}
	}
	return nil
}

func (m *Bolt) Partition(_ context.Context, s string) (*model.Partition, error) {
	var partition model.Partition
	err := m.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(partitionsBucket))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(s))
		if data == nil {
			return nil
		}
		if err := json.Unmarshal(data, &partition); err != nil {
			return fmt.Errorf("failed to unmarshal partition: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get partition: %w", err)
	}
	if partition.ID == "" {
		return nil, errors.ErrPartitionNotFound
	}
	return &partition, nil
}

func (m *Bolt) UpdatePartition(_ context.Context, partition *model.Partition) error {
	boltTx, err := m.db.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	err = m.UpdatePartitionInTx(context.Background(), tx, partition)
	if err != nil {
		return fmt.Errorf("failed to update partition: %w", err)
	}
	return tx.Commit()
}

func (m *Bolt) AllPartitions(ctx context.Context) ([]*model.Partition, error) {
	boltTx, err := m.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	partitions, err := m.AllPartitionsInTx(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all partitions: %w", err)
	}
	return partitions, tx.Commit()
}

func (m *Bolt) AllPartitionsInTx(ctx context.Context, tx storage.Transaction) ([]*model.Partition, error) {
	var partitions []*model.Partition
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(partitionsBucket))
	if bucket == nil {
		return nil, nil
	}
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		var partition model.Partition
		if err := json.Unmarshal(v, &partition); err != nil {
			return nil, fmt.Errorf("failed to unmarshal partition: %w", err)
		}
		partitions = append(partitions, &partition)
	}
	return partitions, nil
}

func (m *Bolt) UpdatePartitionInTx(
	_ context.Context,
	tx storage.Transaction,
	partition *model.Partition,
) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(partitionsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	partitionData, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("failed to marshal partition: %w", err)
	}
	if err := bucket.Put([]byte(partition.ID), partitionData); err != nil {
		return fmt.Errorf("failed to put partition: %w", err)
	}
	return nil
}

func (m *Bolt) PartitionsForTopic(ctx context.Context, topicName string) ([]*model.Partition, error) {
	boltTx, err := m.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	partitions, err := m.PartitionsInTx(ctx, tx, map[string]bool{topicName: true})
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	return partitions, tx.Commit()
}

func (m *Bolt) PartitionsForTopics(ctx context.Context, topicNames []string) ([]*model.Partition, error) {
	boltTx, err := m.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	partitions, err := m.PartitionsInTx(ctx, tx, util.ToSet(topicNames))
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	return partitions, tx.Commit()
}

func (m *Bolt) PartitionsInTx(
	_ context.Context,
	tx storage.Transaction,
	topicNames map[string]bool,
) ([]*model.Partition, error) {
	var partitions []*model.Partition
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(partitionsBucket))
	if bucket == nil {
		return nil, fmt.Errorf("bucket not found")
	}
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		var partition model.Partition
		if err := json.Unmarshal(v, &partition); err != nil {
			return nil, fmt.Errorf("failed to unmarshal partition: %w", err)
		}
		if _, ok := topicNames[partition.TopicName]; ok {
			partitions = append(partitions, &partition)
		}
	}
	return partitions, nil
}

func (m *Bolt) CreateConsumerGroup(ctx context.Context, group *model.ConsumerGroup) error {
	boltTx, err := m.db.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	err = m.CreateConsumerGroupInTx(ctx, tx, group)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	return tx.Commit()
}

func (m *Bolt) CreateConsumerGroupInTx(
	_ context.Context,
	tx storage.Transaction,
	group *model.ConsumerGroup,
) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	groupData, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer group: %w", err)
	}
	if err := bucket.Put([]byte(group.ID), groupData); err != nil {
		return fmt.Errorf("failed to put consumer group: %w", err)
	}
	return nil
}

func (m *Bolt) ConsumerGroup(ctx context.Context, consumerGroupID string) (*model.ConsumerGroup, error) {
	boltTx, err := m.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	consumerGroup, err := m.ConsumerGroupInTx(ctx, tx, consumerGroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group: %w", err)
	}
	return consumerGroup, tx.Commit()
}

func (m *Bolt) PartitionAssignments(ctx context.Context, consumerGroupID string) (map[string][]string, error) {
	boltTx, err := m.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	assignments, err := m.PartitionAssignmentsInTx(ctx, tx, consumerGroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition assignments: %w", err)
	}
	return assignments, tx.Commit()
}

func (m *Bolt) PartitionAssignmentsInTx(
	_ context.Context,
	tx storage.Transaction,
	consumerGroupID string,
) (map[string][]string, error) {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(consumerGroupsBucket))
	if bucket == nil {
		return nil, fmt.Errorf("bucket not found")
	}
	data := bucket.Get([]byte(consumerGroupID))
	if data == nil {
		return nil, fmt.Errorf("consumer group not found")
	}
	var group model.ConsumerGroup
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer group: %w", err)
	}
	assignments := make(map[string][]string)
	for _, consumerID := range util.Keys(group.Consumers) {
		consumer, err := m.ConsumerInTx(context.Background(), tx, consumerID)
		if err != nil {
			return nil, fmt.Errorf("failed to get consumer %s: %w", consumerID, err)
		}
		assignments[consumerID] = consumer.Partitions
	}
	return assignments, nil
}

func (m *Bolt) ConsumerGroupInTx(
	_ context.Context,
	tx storage.Transaction,
	consumerGroupID string,
) (*model.ConsumerGroup, error) {
	var group model.ConsumerGroup
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(consumerGroupsBucket))
	if bucket == nil {
		return nil, errors.ErrConsumerGroupNotFound
	}
	data := bucket.Get([]byte(consumerGroupID))
	if data == nil {
		return nil, errors.ErrConsumerGroupNotFound
	}
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer group: %w", err)
	}
	return &group, nil
}

func (m *Bolt) AddConsumerToGroupInTx(
	_ context.Context,
	tx storage.Transaction,
	group *model.ConsumerGroup,
	consumer *model.Consumer,
) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	group.AddConsumer(consumer.ID)
	groupData, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer group: %w", err)
	}
	if err := bucket.Put([]byte(group.ID), groupData); err != nil {
		return fmt.Errorf("failed to put consumer group: %w", err)
	}
	return nil
}

func (m *Bolt) UpdateConsumerGroup(ctx context.Context, group *model.ConsumerGroup) error {
	boltTx, err := m.db.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	err = m.UpdateConsumerGroupInTx(ctx, tx, group)
	if err != nil {
		return fmt.Errorf("failed to update consumer group: %w", err)
	}
	return tx.Commit()
}

func (m *Bolt) UpdateConsumerGroupInTx(
	_ context.Context,
	tx storage.Transaction,
	group *model.ConsumerGroup,
) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	groupData, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer group: %w", err)
	}
	if err := bucket.Put([]byte(group.ID), groupData); err != nil {
		return fmt.Errorf("failed to put consumer group: %w", err)
	}
	return nil
}

func (m *Bolt) RemoveConsumerFromGroupInTx(
	_ context.Context,
	tx storage.Transaction,
	group *model.ConsumerGroup,
	consumer *model.Consumer,
) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	delete(group.Consumers, consumer.ID)
	groupData, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer group: %w", err)
	}
	if err := bucket.Put([]byte(group.ID), groupData); err != nil {
		return fmt.Errorf("failed to put consumer group: %w", err)
	}
	return nil
}

func (m *Bolt) UpdateConsumer(ctx context.Context, consumer *model.Consumer) error {
	boltTx, err := m.db.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	err = m.UpdateConsumerInTx(ctx, tx, consumer)
	if err != nil {
		return fmt.Errorf("failed to update consumer: %w", err)
	}
	return tx.Commit()
}

func (m *Bolt) UpdateConsumerInTx(_ context.Context, tx storage.Transaction, consumer *model.Consumer) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumersBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	consumerData, err := json.Marshal(consumer)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer: %w", err)
	}
	if err := bucket.Put([]byte(consumer.ID), consumerData); err != nil {
		return fmt.Errorf("failed to put consumer: %w", err)
	}
	return nil
}

func (m *Bolt) CreateConsumer(ctx context.Context, consumer *model.Consumer) error {
	boltTx, err := m.db.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	err = m.CreateConsumerInTx(ctx, tx, consumer)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	return tx.Commit()
}

func (m *Bolt) CreateConsumerInTx(_ context.Context, tx storage.Transaction, consumer *model.Consumer) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumersBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	consumerData, err := json.Marshal(consumer)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer: %w", err)
	}
	if err := bucket.Put([]byte(consumer.ID), consumerData); err != nil {
		return fmt.Errorf("failed to put consumer: %w", err)
	}
	return nil
}

func (m *Bolt) AllConsumers(ctx context.Context) ([]*model.Consumer, error) {
	var consumers []*model.Consumer
	err := m.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(consumersBucket))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var consumer model.Consumer
			if err := json.Unmarshal(v, &consumer); err != nil {
				return fmt.Errorf("failed to unmarshal consumer: %w", err)
			}
			if consumer.IsActive {
				consumers = append(consumers, &consumer)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get consumers: %w", err)
	}
	return consumers, nil
}

func (m *Bolt) Consumer(ctx context.Context, s string) (*model.Consumer, error) {
	boltTx, err := m.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	consumer, err := m.ConsumerInTx(ctx, tx, s)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer: %w", err)
	}
	return consumer, tx.Commit()
}

func (m *Bolt) ConsumerInTx(_ context.Context, tx storage.Transaction, s string) (*model.Consumer, error) {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(consumersBucket))
	if bucket == nil {
		return nil, errors.ErrConsumerNotFound
	}
	data := bucket.Get([]byte(s))
	if data == nil {
		return nil, errors.ErrConsumerNotFound
	}
	var consumer model.Consumer
	if err := json.Unmarshal(data, &consumer); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer: %w", err)
	}
	return &consumer, nil
}

func (m *Bolt) DeleteConsumerInTx(_ context.Context, tx storage.Transaction, consumer *model.Consumer) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumersBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	if err := bucket.Delete([]byte(consumer.ID)); err != nil {
		return fmt.Errorf("failed to delete consumer: %w", err)
	}
	return nil
}

func (b *Bolt) Snapshot(ctx context.Context, w io.Writer) error {
	writeBytes := func(w io.Writer, data []byte) error {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return fmt.Errorf("short write: expected %d, got %d", len(data), n)
		}
		return nil
	}

	tx, err := b.db.Begin(false)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	dbSize := make([]byte, 8)
	size := tx.Size()
	binary.BigEndian.PutUint64(dbSize, uint64(size))

	if err := writeBytes(w, dbSize); err != nil {
		return fmt.Errorf("failed to write db size: %w", err)
	}
	if _, err := tx.WriteTo(w); err != nil {
		return fmt.Errorf("failed to write db to snapshot: %w", err)
	}

	return nil
}

func (b *Bolt) RecoverFromSnapshot(ctx context.Context, r io.Reader) error {
	dbSize := make([]byte, 8)
	_, err := io.ReadFull(r, dbSize)
	if err != nil {
		return fmt.Errorf("failed to read size of db: %w", err)
	}
	dbFileSize := binary.BigEndian.Uint64(dbSize)

	fileDir := filepath.Dir(b.dbPath)
	tempDirPath := filepath.Join(os.TempDir(), "queue", "metadata", fileDir)
	tempDbFilePath := filepath.Join(tempDirPath, "metadata.tmp")
	if err := os.MkdirAll(tempDirPath, 0755); err != nil {
		return fmt.Errorf("failed to create temp db file directory: %w", err)
	}
	file, err := os.Create(tempDbFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temp db file: %w", err)
	}

	_, err = io.Copy(file, io.LimitReader(r, int64(dbFileSize)))
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to copy db file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close temp db file: %w", err)
	}

	if err := os.Rename(tempDbFilePath, b.dbPath); err != nil {
		return fmt.Errorf("failed to rename temp db file: %w", err)
	}
	return nil
}
