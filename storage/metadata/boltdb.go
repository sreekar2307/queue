package metadata

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
	"google.golang.org/protobuf/proto"

	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/storage"
	"github.com/sreekar2307/queue/storage/errors"
	"github.com/sreekar2307/queue/util"

	boltDB "go.etcd.io/bbolt"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
)

type Bolt struct {
	db     *boltDB.DB
	dbPath string
	tracer trace.Tracer
}

func NewBolt(dbPath string, tracer trace.Tracer) *Bolt {
	return &Bolt{
		dbPath: dbPath,
		tracer: tracer,
	}
}

const (
	brokersBucketKey        = "brokers"
	topicsBucketKey         = "topics"
	consumersBucketKey      = "consumers"
	consumerGroupsBucketKey = "consumer_groups"
	partitionsBucketKey     = "partitions"
	commandsBucketKey       = "commands"
	appliedCommandKey       = "applied_command"
)

func (b *Bolt) CreateBrokerInTx(ctx context.Context, tx storage.Transaction, broker *model.Broker) error {
	ctx, span := b.tracer.Start(
		ctx,
		"CreateBrokerInTx",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(brokersBucketKey),
			semconv.DBOperationNameKey.String("PUT"),
		),
	)
	defer span.End()
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		err := fmt.Errorf("invalid transaction type")
		span.RecordError(err)
		return err
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(brokersBucketKey))
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	brokerData, err := proto.Marshal(broker.ToProtoBuf())
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal broker: %w", err)
	}
	if err := bucket.Put(binary.BigEndian.AppendUint64(nil, broker.ID), brokerData); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to put broker: %w", err)
	}
	return nil
}

func (b *Bolt) GetBroker(ctx context.Context, brokerID uint64) (*model.Broker, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"GetBroker",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(brokersBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	broker := new(model.Broker)
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(brokersBucketKey))
		if bucket == nil {
			return nil
		}
		data := bucket.Get(binary.BigEndian.AppendUint64(nil, brokerID))
		if data == nil {
			return nil
		}
		var pbBroker pbTypes.Broker
		if err := proto.Unmarshal(data, &pbBroker); err != nil {
			return fmt.Errorf("failed to unmarshal broker: %w", err)
		}
		broker = model.FromProtoBufBroker(&pbBroker)
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get broker: %w", err)
	}
	if broker.ID == 0 {
		err := errors.ErrBrokerNotFound
		span.RecordError(err)
		return nil, err
	}
	return broker, nil
}

func (b *Bolt) GetBrokers(ctx context.Context, brokerIDs map[uint64]bool) ([]*model.Broker, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"GetBrokers",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(brokersBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	var brokers []*model.Broker
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(brokersBucketKey))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			broker := new(model.Broker)
			var pbBroker pbTypes.Broker
			if err := proto.Unmarshal(v, &pbBroker); err != nil {
				return fmt.Errorf("failed to unmarshal broker: %w", err)
			}
			broker = model.FromProtoBufBroker(&pbBroker)
			if _, ok := brokerIDs[broker.ID]; ok {
				brokers = append(brokers, broker)
			}
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get brokers: %w", err)
	}
	return brokers, nil
}

func (b *Bolt) GetAllBrokers(ctx context.Context) ([]*model.Broker, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"GetAllBrokers",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(brokersBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	var brokers []*model.Broker
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(brokersBucketKey))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var pbBroker pbTypes.Broker
			if err := proto.Unmarshal(v, &pbBroker); err != nil {
				return fmt.Errorf("failed to unmarshal broker: %w", err)
			}
			brokers = append(brokers, model.FromProtoBufBroker(&pbBroker))
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get brokers: %w", err)
	}
	return brokers, nil
}

func (b *Bolt) LastAppliedCommandID(_ context.Context) (uint64, error) {
	var lastAppliedCommandID uint64
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(commandsBucketKey))
		if bucket == nil {
			return nil
		}
		lastAppliedCommand := bucket.Get([]byte(appliedCommandKey))
		if lastAppliedCommand != nil {
			lastAppliedCommandID = binary.BigEndian.Uint64(lastAppliedCommand)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get last applied command ID: %w", err)
	}
	return lastAppliedCommandID, nil
}

func (b *Bolt) Open(_ context.Context) error {
	newDB, err := boltDB.Open(b.dbPath, 0777, nil)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	b.db = newDB
	return nil
}

func (b *Bolt) Close(_ context.Context) error {
	if err := b.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	return nil
}

func (b *Bolt) BeginTransaction(_ context.Context, forWrite bool) (storage.Transaction, error) {
	tx, err := b.db.Begin(forWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &storage.BoltDbTransactionWrapper{BoltTx: tx}, nil
}

func (b *Bolt) CheckCommandAppliedInTx(_ context.Context, tx storage.Transaction, commandID uint64) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	commandsBucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(commandsBucketKey))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	lastAppliedCommand := commandsBucket.Get([]byte(appliedCommandKey))
	if lastAppliedCommand != nil {
		lastAppliedCommandID := binary.BigEndian.Uint64(lastAppliedCommand)
		if commandID <= lastAppliedCommandID {
			return errors.ErrDuplicateCommand
		}
	}
	return nil
}

func (b *Bolt) UpdateCommandAppliedInTx(_ context.Context, tx storage.Transaction, commandID uint64) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	commandsBucket := boltTx.BoltTx.Bucket([]byte(commandsBucketKey))
	if commandsBucket == nil {
		return fmt.Errorf("commands bucket not found")
	}
	if err := commandsBucket.Put([]byte(appliedCommandKey), binary.BigEndian.AppendUint64(nil, commandID)); err != nil {
		return fmt.Errorf("failed to put command ID: %w", err)
	}
	return nil
}

func (b *Bolt) CreateTopicInTx(_ context.Context, tx storage.Transaction, topic *model.Topic) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(topicsBucketKey))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	topicData, err := proto.Marshal(topic.ToProtoBuf())
	if err != nil {
		return fmt.Errorf("failed to marshal topic: %w", err)
	}
	if err := bucket.Put([]byte(topic.Name), topicData); err != nil {
		return fmt.Errorf("failed to put topic: %w", err)
	}
	return nil
}

func (b *Bolt) Topic(ctx context.Context, s string) (*model.Topic, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"Topic",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(topicsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	topic := new(model.Topic)
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(topicsBucketKey))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(s))
		if data == nil {
			return nil
		}
		var pbTopic pbTypes.Topic
		if err := proto.Unmarshal(data, &pbTopic); err != nil {
			return fmt.Errorf("failed to unmarshal topic: %w", err)
		}
		topic = model.FromProtoBufTopic(&pbTopic)
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get topic: %w", err)
	}
	if topic.Name == "" {
		err := errors.ErrTopicNotFound
		span.RecordError(err)
		return nil, errors.ErrTopicNotFound
	}
	partitions, err := b.PartitionsForTopic(ctx, topic.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	topic.NumberOfPartitions = uint64(len(partitions))
	return topic, nil
}

func (b *Bolt) TopicInTx(ctx context.Context, tx storage.Transaction, s string) (*model.Topic, error) {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(topicsBucketKey))
	if bucket == nil {
		return nil, errors.ErrTopicNotFound
	}
	data := bucket.Get([]byte(s))
	if data == nil {
		return nil, errors.ErrTopicNotFound
	}
	var (
		topic   = new(model.Topic)
		pbTopic pbTypes.Topic
	)
	if err := proto.Unmarshal(data, &pbTopic); err != nil {
		return nil, fmt.Errorf("failed to unmarshal topic: %w", err)
	}
	topic = model.FromProtoBufTopic(&pbTopic)
	partitions, err := b.PartitionsInTx(ctx, tx, map[string]bool{s: true})
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	topic.NumberOfPartitions = uint64(len(partitions))
	return topic, nil
}

func (b *Bolt) Topics(ctx context.Context, topicNames []string) ([]*model.Topic, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"Topics",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(topicsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	var topics []*model.Topic
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(topicsBucketKey))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var pbTopic pbTypes.Topic
			if err := proto.Unmarshal(v, &pbTopic); err != nil {
				return fmt.Errorf("failed to unmarshal topic: %w", err)
			}
			topic := model.FromProtoBufTopic(&pbTopic)
			found := false
			for _, name := range topicNames {
				if name == topic.Name {
					found = true
					break
				}
			}
			if found {
				topics = append(topics, topic)
			}
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get topics: %w", err)
	}
	return topics, nil
}

func (b *Bolt) AllTopics(ctx context.Context) ([]*model.Topic, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"AllTopics",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(topicsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	var topics []*model.Topic
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(topicsBucketKey))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var pbTopic pbTypes.Topic
			if err := proto.Unmarshal(v, &pbTopic); err != nil {
				return fmt.Errorf("failed to unmarshal topic: %w", err)
			}
			topics = append(topics, model.FromProtoBufTopic(&pbTopic))
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get topics: %w", err)
	}
	return topics, nil
}

func (b *Bolt) CreatePartitionsInTx(
	_ context.Context,
	transaction storage.Transaction,
	partitions []*model.Partition,
) error {
	tx, ok := transaction.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte(partitionsBucketKey))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	for _, partition := range partitions {
		partitionData, err := proto.Marshal(partition.ToProtoBuf())
		if err != nil {
			return fmt.Errorf("failed to marshal partition: %w", err)
		}
		if err := bucket.Put([]byte(partition.ID), partitionData); err != nil {
			return fmt.Errorf("failed to put partition: %w", err)
		}
	}
	return nil
}

func (b *Bolt) Partition(_ context.Context, s string) (*model.Partition, error) {
	partition := new(model.Partition)
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(partitionsBucketKey))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(s))
		if data == nil {
			return nil
		}
		var pbPartition pbTypes.Partition
		if err := proto.Unmarshal(data, &pbPartition); err != nil {
			return fmt.Errorf("failed to unmarshal partition: %w", err)
		}
		partition = model.FromProtoBufPartition(&pbPartition)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get partition: %w", err)
	}
	if partition.ID == "" {
		return nil, errors.ErrPartitionNotFound
	}
	return partition, nil
}

func (b *Bolt) AllPartitions(ctx context.Context) ([]*model.Partition, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"AllPartitions",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(partitionsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	var partitions []*model.Partition
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(partitionsBucketKey))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var pbPartition pbTypes.Partition
			if err := proto.Unmarshal(v, &pbPartition); err != nil {
				return fmt.Errorf("failed to unmarshal partition: %w", err)
			}
			partitions = append(partitions, model.FromProtoBufPartition(&pbPartition))
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	return partitions, nil
}

func (b *Bolt) AllPartitionsInTx(_ context.Context, tx storage.Transaction) ([]*model.Partition, error) {
	var partitions []*model.Partition
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(partitionsBucketKey))
	if bucket == nil {
		return nil, nil
	}
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		var pbPartition pbTypes.Partition
		if err := proto.Unmarshal(v, &pbPartition); err != nil {
			return nil, fmt.Errorf("failed to unmarshal partition: %w", err)
		}
		partitions = append(partitions, model.FromProtoBufPartition(&pbPartition))
	}
	return partitions, nil
}

func (b *Bolt) UpdatePartitionInTx(
	_ context.Context,
	tx storage.Transaction,
	partition *model.Partition,
) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(partitionsBucketKey))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	partitionData, err := proto.Marshal(partition.ToProtoBuf())
	if err != nil {
		return fmt.Errorf("failed to marshal partition: %w", err)
	}
	if err := bucket.Put([]byte(partition.ID), partitionData); err != nil {
		return fmt.Errorf("failed to put partition: %w", err)
	}
	return nil
}

func (b *Bolt) PartitionsForTopic(ctx context.Context, topicName string) ([]*model.Partition, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"PartitionsForTopic",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(partitionsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	var partitions []*model.Partition
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(partitionsBucketKey))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var pbPartition pbTypes.Partition
			if err := proto.Unmarshal(v, &pbPartition); err != nil {
				return fmt.Errorf("failed to unmarshal partition: %w", err)
			}
			partition := model.FromProtoBufPartition(&pbPartition)
			if partition.TopicName == topicName {
				partitions = append(partitions, partition)
			}
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	return partitions, nil
}

func (b *Bolt) PartitionsForTopics(ctx context.Context, topicNames []string) ([]*model.Partition, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"PartitionsForTopics",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(partitionsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	boltTx, err := b.db.Begin(false)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	partitions, err := b.PartitionsInTx(ctx, tx, util.ToSet(topicNames))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return partitions, nil
}

func (b *Bolt) PartitionsInTx(
	_ context.Context,
	tx storage.Transaction,
	topicNames map[string]bool,
) ([]*model.Partition, error) {
	var partitions []*model.Partition
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(partitionsBucketKey))
	if bucket == nil {
		return nil, fmt.Errorf("bucket not found")
	}
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		var pbPartition pbTypes.Partition
		if err := proto.Unmarshal(v, &pbPartition); err != nil {
			return nil, fmt.Errorf("failed to unmarshal partition: %w", err)
		}
		if _, ok := topicNames[pbPartition.Topic]; ok {
			partitions = append(partitions, model.FromProtoBufPartition(&pbPartition))
		}
	}
	return partitions, nil
}

func (b *Bolt) CreateConsumerGroupInTx(
	_ context.Context,
	tx storage.Transaction,
	group *model.ConsumerGroup,
) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucketKey))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	groupData, err := proto.Marshal(group.ToProtoBuf())
	if err != nil {
		return fmt.Errorf("failed to marshal consumer group: %w", err)
	}
	if err := bucket.Put([]byte(group.ID), groupData); err != nil {
		return fmt.Errorf("failed to put consumer group: %w", err)
	}
	return nil
}

func (b *Bolt) ConsumerGroup(ctx context.Context, consumerGroupID string) (*model.ConsumerGroup, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"ConsumerGroup",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumerGroupsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	boltTx, err := b.db.Begin(false)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	consumerGroup, err := b.ConsumerGroupInTx(ctx, tx, consumerGroupID)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get consumer group: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return consumerGroup, nil
}

func (b *Bolt) PartitionAssignmentsInTx(ctx context.Context, tx storage.Transaction, consumerGroupID string) (map[string][]string, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"PartitionAssignmentsInTx",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumerGroupsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		err := fmt.Errorf("invalid transaction type")
		span.RecordError(err)
		return nil, err
	}
	bucket := boltTx.BoltTx.Bucket([]byte(consumerGroupsBucketKey))
	if bucket == nil {
		err := fmt.Errorf("bucket not found")
		span.RecordError(err)
		return nil, err
	}
	data := bucket.Get([]byte(consumerGroupID))
	if data == nil {
		err := fmt.Errorf("consumer group not found")
		span.RecordError(err)
		return nil, err
	}
	var pbGroup pbTypes.ConsumerGroup
	if err := proto.Unmarshal(data, &pbGroup); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to unmarshal consumer group: %w", err)
	}
	group := model.FromProtoBufConsumerGroup(&pbGroup)
	assignments := make(map[string][]string)
	for _, consumerID := range util.Keys(group.Consumers) {
		consumer, err := b.ConsumerInTx(ctx, tx, consumerID)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to get consumer %s: %w", consumerID, err)
		}
		assignments[consumerID] = consumer.Partitions
	}
	return assignments, nil
}

func (b *Bolt) ConsumerGroupInTx(ctx context.Context, tx storage.Transaction, consumerGroupID string) (*model.ConsumerGroup, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"ConsumerGroupInTx",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumerGroupsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		err := fmt.Errorf("invalid transaction type")
		span.RecordError(err)
		return nil, err
	}
	bucket := boltTx.BoltTx.Bucket([]byte(consumerGroupsBucketKey))
	if bucket == nil {
		err := errors.ErrConsumerGroupNotFound
		span.RecordError(err)
		return nil, err
	}
	data := bucket.Get([]byte(consumerGroupID))
	if data == nil {
		err := errors.ErrConsumerGroupNotFound
		span.RecordError(err)
		return nil, err
	}
	var pbGroup pbTypes.ConsumerGroup
	if err := proto.Unmarshal(data, &pbGroup); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to unmarshal consumer group: %w", err)
	}
	return model.FromProtoBufConsumerGroup(&pbGroup), nil
}

func (b *Bolt) AddConsumerToGroupInTx(ctx context.Context, tx storage.Transaction, group *model.ConsumerGroup, consumer *model.Consumer) error {
	ctx, span := b.tracer.Start(
		ctx,
		"AddConsumerToGroupInTx",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumerGroupsBucketKey),
			semconv.DBOperationNameKey.String("PUT"),
		),
	)
	defer span.End()
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		err := fmt.Errorf("invalid transaction type")
		span.RecordError(err)
		return err
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucketKey))
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	group.AddConsumer(consumer.ID)
	groupData, err := proto.Marshal(group.ToProtoBuf())
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal consumer group: %w", err)
	}
	if err := bucket.Put([]byte(group.ID), groupData); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to put consumer group: %w", err)
	}
	return nil
}

func (b *Bolt) UpdateConsumerGroupInTx(ctx context.Context, tx storage.Transaction, group *model.ConsumerGroup) error {
	ctx, span := b.tracer.Start(
		ctx,
		"UpdateConsumerGroupInTx",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumerGroupsBucketKey),
			semconv.DBOperationNameKey.String("PUT"),
		),
	)
	defer span.End()
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		err := fmt.Errorf("invalid transaction type")
		span.RecordError(err)
		return err
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucketKey))
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	groupData, err := proto.Marshal(group.ToProtoBuf())
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal consumer group: %w", err)
	}
	if err := bucket.Put([]byte(group.ID), groupData); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to put consumer group: %w", err)
	}
	return nil
}

func (b *Bolt) RemoveConsumerFromGroupInTx(ctx context.Context, tx storage.Transaction, group *model.ConsumerGroup, consumer *model.Consumer) error {
	ctx, span := b.tracer.Start(
		ctx,
		"RemoveConsumerFromGroupInTx",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumerGroupsBucketKey),
			semconv.DBOperationNameKey.String("PUT"),
		),
	)
	defer span.End()
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		err := fmt.Errorf("invalid transaction type")
		span.RecordError(err)
		return err
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumerGroupsBucketKey))
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	delete(group.Consumers, consumer.ID)
	groupData, err := proto.Marshal(group.ToProtoBuf())
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal consumer group: %w", err)
	}
	if err := bucket.Put([]byte(group.ID), groupData); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to put consumer group: %w", err)
	}
	return nil
}

func (b *Bolt) UpdateConsumer(ctx context.Context, commandID uint64, consumer *model.Consumer) error {
	ctx, span := b.tracer.Start(
		ctx,
		"UpdateConsumer",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumersBucketKey),
			semconv.DBOperationNameKey.String("PUT"),
		),
	)
	defer span.End()
	boltTx, err := b.db.Begin(true)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	if err := b.CheckCommandAppliedInTx(ctx, tx, commandID); err != nil {
		span.RecordError(err)
		return err
	}
	err = b.UpdateConsumerInTx(ctx, tx, consumer)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to update consumer: %w", err)
	}
	if err := b.UpdateCommandAppliedInTx(ctx, tx, commandID); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to update command applied: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (b *Bolt) UpdateConsumerInTx(_ context.Context, tx storage.Transaction, consumer *model.Consumer) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumersBucketKey))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	consumerData, err := proto.Marshal(consumer.ToProtoBuf())
	if err != nil {
		return fmt.Errorf("failed to marshal consumer: %w", err)
	}
	if err := bucket.Put([]byte(consumer.ID), consumerData); err != nil {
		return fmt.Errorf("failed to put consumer: %w", err)
	}
	return nil
}

func (b *Bolt) CreateConsumerInTx(_ context.Context, tx storage.Transaction, consumer *model.Consumer) error {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	bucket, err := boltTx.BoltTx.CreateBucketIfNotExists([]byte(consumersBucketKey))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	consumerData, err := proto.Marshal(consumer.ToProtoBuf())
	if err != nil {
		return fmt.Errorf("failed to marshal consumer: %w", err)
	}
	if err := bucket.Put([]byte(consumer.ID), consumerData); err != nil {
		return fmt.Errorf("failed to put consumer: %w", err)
	}
	return nil
}

func (b *Bolt) AllConsumers(ctx context.Context) ([]*model.Consumer, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"AllConsumers",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumersBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	var consumers []*model.Consumer
	err := b.db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(consumersBucketKey))
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var pbConsumer pbTypes.Consumer
			if err := proto.Unmarshal(v, &pbConsumer); err != nil {
				return fmt.Errorf("failed to unmarshal consumer: %w", err)
			}
			consumer := model.FromProtoBufConsumer(&pbConsumer)
			if consumer.IsActive {
				consumers = append(consumers, consumer)
			}
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get consumers: %w", err)
	}
	return consumers, nil
}

func (b *Bolt) Consumer(ctx context.Context, s string) (*model.Consumer, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"Consumer",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
			semconv.DBCollectionNameKey.String(consumersBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	boltTx, err := b.db.Begin(true)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	tx := &storage.BoltDbTransactionWrapper{BoltTx: boltTx}
	defer tx.BoltTx.Rollback()
	consumer, err := b.ConsumerInTx(ctx, tx, s)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get consumer: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return consumer, nil
}

func (b *Bolt) ConsumerInTx(_ context.Context, tx storage.Transaction, s string) (*model.Consumer, error) {
	boltTx, ok := tx.(*storage.BoltDbTransactionWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}
	bucket := boltTx.BoltTx.Bucket([]byte(consumersBucketKey))
	if bucket == nil {
		return nil, errors.ErrConsumerNotFound
	}
	data := bucket.Get([]byte(s))
	if data == nil {
		return nil, errors.ErrConsumerNotFound
	}
	var pbConsumer pbTypes.Consumer
	if err := proto.Unmarshal(data, &pbConsumer); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer: %w", err)
	}
	return model.FromProtoBufConsumer(&pbConsumer), nil
}

func (b *Bolt) Snapshot(ctx context.Context, w io.Writer) error {
	ctx, span := b.tracer.Start(
		ctx,
		"Snapshot metadata",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
		),
	)
	defer span.End()
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
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	dbSize := make([]byte, 8)
	size := tx.Size()
	binary.BigEndian.PutUint64(dbSize, uint64(size))

	if err := writeBytes(w, dbSize); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to write db size: %w", err)
	}
	if _, err := tx.WriteTo(w); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to write db to snapshot: %w", err)
	}

	return nil
}

func (b *Bolt) RecoverFromSnapshot(ctx context.Context, r io.Reader) error {
	ctx, span := b.tracer.Start(
		ctx,
		"RecoverFromSnapshot metadata",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.dbPath),
		),
	)
	defer span.End()
	dbSize := make([]byte, 8)
	_, err := io.ReadFull(r, dbSize)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to read size of db: %w", err)
	}
	dbFileSize := binary.BigEndian.Uint64(dbSize)

	fileDir := filepath.Dir(b.dbPath)
	tempDirPath := filepath.Join(os.TempDir(), "queue", "metadata", fileDir)
	tempDbFilePath := filepath.Join(tempDirPath, "metadata.tmp")
	if err := os.MkdirAll(tempDirPath, 0755); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create temp db file directory: %w", err)
	}
	file, err := os.Create(tempDbFilePath)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create temp db file: %w", err)
	}

	_, err = io.Copy(file, io.LimitReader(r, int64(dbFileSize)))
	if err != nil {
		file.Close()
		span.RecordError(err)
		return fmt.Errorf("failed to copy db file: %w", err)
	}
	if err := file.Close(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to close temp db file: %w", err)
	}

	if err := os.Rename(tempDbFilePath, b.dbPath); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to rename temp db file: %w", err)
	}
	return nil
}
