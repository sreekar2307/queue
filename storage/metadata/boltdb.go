package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"queue/model"
	"queue/storage"
	"queue/storage/errors"
	"sync"

	boltDB "go.etcd.io/bbolt"
)

type Bolt struct {
	db     *boltDB.DB
	dbPath string

	mu sync.RWMutex
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
	m.mu.Lock()
	defer m.mu.Unlock()
	newDB, err := boltDB.Open(m.dbPath, 0777, nil)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	m.db = newDB
	return nil
}

func (m *Bolt) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func (m *Bolt) CreateTopicInTx(_ context.Context, transaction storage.Transaction, topic *model.Topic) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(topicsBucket))
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
	partitions, err := m.Partitions(ctx, topic.Name)
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
			partitions, err := m.Partitions(ctx, topic.Name)
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
			partitions, err := m.Partitions(ctx, topic.Name)
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
	m.mu.Lock()
	defer m.mu.Unlock()
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
			return fmt.Errorf("partition not found")
		}
		if err := json.Unmarshal(data, &partition); err != nil {
			return fmt.Errorf("failed to unmarshal partition: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get partition: %w", err)
	}
	return &partition, nil
}

func (m *Bolt) Partitions(_ context.Context, topicName string) ([]*model.Partition, error) {
	var partitions []*model.Partition
	err := m.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(partitionsBucket))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var partition model.Partition
			if err := json.Unmarshal(v, &partition); err != nil {
				return fmt.Errorf("failed to unmarshal partition: %w", err)
			}
			if partition.TopicName == topicName {
				partitions = append(partitions, &partition)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
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

func (m *Bolt) CreateConsumerGroupInTx(_ context.Context, transaction storage.Transaction, group *model.ConsumerGroup) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
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

func (m *Bolt) ConsumerGroupInTx(_ context.Context, transaction storage.Transaction, consumerGroupID string) (*model.ConsumerGroup, error) {
	var group model.ConsumerGroup
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := tx.BoltTx.Bucket([]byte(consumerGroupsBucket))
	if bucket == nil {
		return nil, fmt.Errorf("bucket not found")
	}
	data := bucket.Get([]byte(consumerGroupID))
	if data == nil {
		return nil, fmt.Errorf("consumer group not found")
	}
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer group: %w", err)
	}
	return &group, nil
}

func (m *Bolt) AddConsumerToGroupInTx(_ context.Context, transaction storage.Transaction, group *model.ConsumerGroup, consumer *model.Consumer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	group.Consumers[consumer.ID] = true
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

func (m *Bolt) UpdateConsumerGroupInTx(_ context.Context, transaction storage.Transaction, group *model.ConsumerGroup) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
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

func (m *Bolt) RemoveConsumerFromGroupInTx(_ context.Context, transaction storage.Transaction, group *model.ConsumerGroup, consumer *model.Consumer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
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

func (m *Bolt) UpdateConsumerInTx(_ context.Context, transaction storage.Transaction, consumer *model.Consumer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(consumersBucket))
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

func (m *Bolt) CreateConsumerInTx(_ context.Context, transaction storage.Transaction, consumer *model.Consumer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(consumersBucket))
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

func (m *Bolt) ConsumerInTx(_ context.Context, transaction storage.Transaction, s string) (*model.Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := tx.BoltTx.Bucket([]byte(consumersBucket))
	if bucket == nil {
		return nil, fmt.Errorf("bucket not found")
	}
	data := bucket.Get([]byte(s))
	if data == nil {
		return nil, fmt.Errorf("consumer not found")
	}
	var consumer model.Consumer
	if err := json.Unmarshal(data, &consumer); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer: %w", err)
	}
	return &consumer, nil
}

func (m *Bolt) DeleteConsumerInTx(_ context.Context, transaction storage.Transaction, consumer *model.Consumer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(consumersBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	if err := bucket.Delete([]byte(consumer.ID)); err != nil {
		return fmt.Errorf("failed to delete consumer: %w", err)
	}
	return nil
}
