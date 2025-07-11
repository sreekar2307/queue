package message

import (
	"bytes"
	"context"
	"encoding/binary"
	stdErrors "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"

	"github.com/sreekar2307/queue/logger"
	"go.opentelemetry.io/otel/trace"

	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
	"google.golang.org/protobuf/proto"

	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/storage/errors"

	boltDB "go.etcd.io/bbolt"
)

type Bolt struct {
	PartitionsPath string
	mu             sync.RWMutex
	dbs            map[string]*boltDB.DB
	log            logger.Logger
	tracer         trace.Tracer
}

func NewBolt(partitionPath string, tracer trace.Tracer, log logger.Logger) *Bolt {
	return &Bolt{
		PartitionsPath: partitionPath,
		dbs:            make(map[string]*boltDB.DB),
		log:            log,
		tracer:         tracer,
	}
}

const (
	messagesBucketKey               = "messages"
	messageStatsBucketKey           = "messages_stats"
	messageStatsPartitionsBucketKey = "partitions"
	commandsBucketKey               = "commands"
	appliedCommandKey               = "applied_command"
)

func (b *Bolt) Close(ctx context.Context) error {
	ctx, span := b.tracer.Start(
		ctx,
		"Close BoltDB",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
		),
	)
	defer span.End()
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, db := range b.dbs {
		if err := db.Close(); err != nil {
			err = fmt.Errorf("failed to close database: %w", err)
			span.RecordError(err)
			return err
		}
	}
	return nil
}

func (b *Bolt) LastAppliedCommandID(
	ctx context.Context,
	partitions []string,
) (uint64, error) {
	var maxLastAppliedCommandID uint64
	ctx, span := b.tracer.Start(
		ctx,
		"LastAppliedCommandID commands",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
			semconv.DBCollectionNameKey.String(commandsBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	for _, partitionID := range partitions {
		db, err := b.getDBForPartition(ctx, partitionID)
		if err != nil {
			span.RecordError(err)
			return 0, fmt.Errorf("failed to get database for partition: %w", err)
		}
		err = db.View(func(tx *boltDB.Tx) error {
			commandsBucket := tx.Bucket([]byte(commandsBucketKey))
			if commandsBucket == nil {
				return nil
			}
			lastAppliedCommand := commandsBucket.Get([]byte(appliedCommandKey))
			if lastAppliedCommand == nil {
				return nil
			}
			lastAppliedCommandID := binary.BigEndian.Uint64(lastAppliedCommand)
			maxLastAppliedCommandID = max(maxLastAppliedCommandID, lastAppliedCommandID)
			return nil
		})
		if err != nil {
			span.RecordError(err)
			return 0, fmt.Errorf("failed to get last applied command ID: %w", err)
		}
	}
	return maxLastAppliedCommandID, nil
}

func (b *Bolt) AppendMessage(
	ctx context.Context,
	commandID uint64,
	message *model.Message,
) error {
	ctx, span := b.tracer.Start(
		ctx,
		"AppendMessage messages",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
			semconv.DBCollectionNameKey.String(messagesBucketKey),
			semconv.DBOperationNameKey.String("PUT"),
		),
	)
	defer span.End()
	db, err := b.getDBForPartition(ctx, message.PartitionID)
	if err != nil {
		err = fmt.Errorf("failed to get database for partition: %w", err)
		span.RecordError(err)
		return err
	}
	if len(message.ID) == 0 {
		err = fmt.Errorf("message id is not set")
		span.RecordError(err)
		return err
	}
	err = db.Update(func(tx *boltDB.Tx) error {
		commandsBucket, err := tx.CreateBucketIfNotExists([]byte(commandsBucketKey))
		if err != nil {
			return fmt.Errorf("failed to create commands bucket: %w", err)
		}
		lastAppliedCommand := commandsBucket.Get([]byte(appliedCommandKey))
		if lastAppliedCommand != nil {
			lastAppliedCommandID := binary.BigEndian.Uint64(lastAppliedCommand)
			if lastAppliedCommandID >= commandID {
				return errors.ErrDuplicateCommand
			}
		}
		if err := commandsBucket.Put([]byte(appliedCommandKey), binary.BigEndian.AppendUint64(nil, commandID)); err != nil {
			return fmt.Errorf("storing command id: %w", err)
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(messagesBucketKey))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		messageData, err := proto.Marshal(message.ToProtoBuf())
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		if err := bucket.Put(message.ID, slices.Clone(messageData)); err != nil {
			return fmt.Errorf("failed to put message: %w", err)
		}
		return nil
	})
	span.RecordError(err)
	return err
}

func (b *Bolt) MessageAtIndex(ctx context.Context, partition *model.Partition, messageID []byte) (*model.Message, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"MessageAtIndex messages",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
			semconv.DBCollectionNameKey.String(messagesBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	db, err := b.getDBForPartition(ctx, partition.ID)
	if err != nil {
		err = fmt.Errorf("failed to get database for partition: %w", err)
		span.RecordError(err)
		return nil, err
	}
	message := new(model.Message)
	err = db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(messagesBucketKey))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		data := bucket.Get(messageID)
		if data == nil {
			return fmt.Errorf("message not found")
		}
		var messagePb pbTypes.Message
		if err := proto.Unmarshal(data, &messagePb); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		message = model.FromProtoBufMessage(&messagePb)
		return nil
	})
	if err != nil {
		err = fmt.Errorf("failed to get message: %w", err)
		span.RecordError(err)
		return nil, err
	}
	return message, nil
}

func (b *Bolt) getDBForPartition(ctx context.Context, partitionKey string) (*boltDB.DB, error) {
	b.mu.RLock()
	messagesDB, ok := b.dbs[partitionKey]
	b.mu.RUnlock()
	if ok {
		return messagesDB, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Double check
	if db, ok := b.dbs[partitionKey]; ok {
		return db, nil
	}
	path := filepath.Join(b.PartitionsPath, partitionKey)
	b.log.Info(ctx, "opening database", logger.NewAttr("path", path))
	newDB, err := boltDB.Open(path, 0777, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	b.dbs[partitionKey] = newDB
	return newDB, nil
}

func (b *Bolt) AckMessage(
	ctx context.Context,
	commandID uint64,
	message *model.Message,
	group *model.ConsumerGroup,
) error {
	ctx, span := b.tracer.Start(
		ctx,
		"AckMessage messages",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
			semconv.DBCollectionNameKey.String(messageStatsBucketKey),
			semconv.DBOperationNameKey.String("PUT"),
		),
	)
	defer span.End()
	db, err := b.getDBForPartition(ctx, message.PartitionID)
	if err != nil {
		err = fmt.Errorf("failed to get database for partition: %w", err)
		span.RecordError(err)
		return err
	}
	err = db.Update(func(tx *boltDB.Tx) error {
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		commandsBucket, err := tx.CreateBucketIfNotExists([]byte(commandsBucketKey))
		if err != nil {
			return fmt.Errorf("failed to create commands bucket: %w", err)
		}
		lastAppliedCommand := commandsBucket.Get([]byte(appliedCommandKey))
		if lastAppliedCommand != nil {
			lastAppliedCommandID := binary.BigEndian.Uint64(lastAppliedCommand)
			if lastAppliedCommandID >= commandID {
				return errors.ErrDuplicateCommand
			}
		}
		if err := commandsBucket.Put([]byte(appliedCommandKey), binary.BigEndian.AppendUint64(nil, commandID)); err != nil {
			return fmt.Errorf("storing command id: %w", err)
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(messageStatsBucketKey))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		partitionBucket, err := bucket.CreateBucketIfNotExists([]byte(messageStatsPartitionsBucketKey))
		if err != nil {
			return fmt.Errorf("failed to create partition bucket: %w", err)
		}
		if err := partitionBucket.Put([]byte(group.ID), message.ID); err != nil {
			return fmt.Errorf("failed to put acked message: %w", err)
		}
		return nil
	})
	span.RecordError(err)
	return err
}

func (b *Bolt) LastMessageID(ctx context.Context, partitionKey string) ([]byte, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"LastMessageID messages",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
			semconv.DBCollectionNameKey.String(messagesBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	db, err := b.getDBForPartition(ctx, partitionKey)
	if err != nil {
		err = fmt.Errorf("failed to get database for partition: %w", err)
		span.RecordError(err)
		return nil, err
	}
	var lastMessageID []byte
	db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(messagesBucketKey))
		if bucket == nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		lastMessageID, _ = bucket.Cursor().Last()
		return nil
	})
	return lastMessageID, nil
}

func (b *Bolt) NextUnAckedMessageID(ctx context.Context, partition *model.Partition, group *model.ConsumerGroup) ([]byte, error) {
	ctx, span := b.tracer.Start(
		ctx,
		"NextUnAckedMessageID messages",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
			semconv.DBCollectionNameKey.String(messagesBucketKey),
			semconv.DBOperationNameKey.String("GET"),
		),
	)
	defer span.End()
	db, err := b.getDBForPartition(ctx, partition.ID)
	if err != nil {
		err = fmt.Errorf("failed to get database for partition: %w", err)
		span.RecordError(err)
		return nil, err
	}
	var nextMesssageID []byte
	err = db.Update(func(tx *boltDB.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(messageStatsBucketKey))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		partitionBucket, err := bucket.CreateBucketIfNotExists([]byte(messageStatsPartitionsBucketKey))
		if err != nil {
			return fmt.Errorf("failed to create partition bucket: %w", err)
		}
		lastAckedMsgId := partitionBucket.Get([]byte(group.ID))
		msgBucket := tx.Bucket([]byte(messagesBucketKey))
		if msgBucket == nil {
			return nil
		}
		lastMsgId, _ := msgBucket.Cursor().Last()
		cursor := msgBucket.Cursor()
		if lastAckedMsgId == nil {
			nextMesssageID, _ = cursor.First()
			return nil
		}
		if !bytes.Equal(lastAckedMsgId, lastMsgId) {
			cursor.Seek(lastAckedMsgId)
			nextMesssageID, _ = cursor.Next()
		}
		return nil
	})
	if err != nil {
		err = fmt.Errorf("failed to get next unacked message ID: %w", err)
		span.RecordError(err)
		return nil, err
	}
	if nextMesssageID == nil {
		err = errors.ErrNoMessageFound
		span.RecordError(err)
		return nil, err
	}
	return nextMesssageID, nil
}

func (b *Bolt) Snapshot(ctx context.Context, w io.Writer) error {
	ctx, span := b.tracer.Start(
		ctx,
		"Snapshot BoltDB",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
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

	for partitionID, db := range b.dbs {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			span.RecordError(err)
			return err
		default:
			partition := []byte(partitionID)
			partitionIDSize := make([]byte, 8)
			binary.BigEndian.PutUint64(partitionIDSize, uint64(len(partition)))

			if err := writeBytes(w, partitionIDSize); err != nil {
				err = fmt.Errorf("failed to write partition ID size: %w", err)
				span.RecordError(err)
				return err
			}
			if err := writeBytes(w, partition); err != nil {
				err = fmt.Errorf("failed to write partition ID: %w", err)
				span.RecordError(err)
				return err
			}

			tx, err := db.Begin(false)
			if err != nil {
				err = fmt.Errorf("failed to begin transaction: %w", err)
				span.RecordError(err)
				return err
			}
			defer tx.Rollback()

			dbSize := make([]byte, 8)
			size := tx.Size()
			binary.BigEndian.PutUint64(dbSize, uint64(size))

			if err := writeBytes(w, dbSize); err != nil {
				err = fmt.Errorf("failed to write db size: %w", err)
				span.RecordError(err)
				return err
			}
			if _, err := tx.WriteTo(w); err != nil {
				err = fmt.Errorf("failed to write db to snapshot: %w", err)
				span.RecordError(err)
				return err
			}

		}
	}
	return nil
}

func (b *Bolt) RecoverFromSnapshot(ctx context.Context, r io.Reader) error {
	ctx, span := b.tracer.Start(
		ctx,
		"RecoverFromSnapshot BoltDB",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String("boltdb"),
			semconv.DBNamespaceKey.String(b.PartitionsPath),
		),
	)
	defer span.End()
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			span.RecordError(err)
			return err
		default:
			partitionIDNameSize := make([]byte, 8)
			_, err := io.ReadFull(r, partitionIDNameSize)
			if stdErrors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				err = fmt.Errorf("failed to read partition ID size from snapshot: %w", err)
				span.RecordError(err)
				return err
			}

			partitionIDSize := binary.BigEndian.Uint64(partitionIDNameSize)
			partitionID := make([]byte, int(partitionIDSize))
			_, err = io.ReadFull(r, partitionID)
			if err != nil {
				err = fmt.Errorf("failed to read partition ID from snapshot: %w", err)
				span.RecordError(err)
				return err
			}

			dbSize := make([]byte, 8)
			_, err = io.ReadFull(r, dbSize)
			if err != nil {
				err = fmt.Errorf("failed to read size of db: %w", err)
				span.RecordError(err)
				return err
			}
			dbFileSize := binary.BigEndian.Uint64(dbSize)

			fileDir := filepath.Dir(b.PartitionsPath)
			tempDirPath := filepath.Join(os.TempDir(), "queue", "messages", fileDir)
			tempDbFilePath := filepath.Join(tempDirPath, string(partitionID)+".tmp")
			if err := os.MkdirAll(tempDirPath, 0777); err != nil {
				err = fmt.Errorf("failed to create temp db file directory: %w", err)
				span.RecordError(err)
				return err
			}
			file, err := os.Create(tempDbFilePath)
			if err != nil {
				err = fmt.Errorf("failed to create temp db file: %w", err)
				span.RecordError(err)
				return err
			}

			_, err = io.Copy(file, io.LimitReader(r, int64(dbFileSize)))
			if err != nil {
				file.Close()
				err = fmt.Errorf("failed to copy db file: %w", err)
				span.RecordError(err)
				return err
			}
			if err := file.Close(); err != nil {
				err = fmt.Errorf("failed to close temp db file: %w", err)
				span.RecordError(err)
				return err
			}

			dbFilePath := filepath.Join(b.PartitionsPath, string(partitionID))
			if err := os.Rename(tempDbFilePath, dbFilePath); err != nil {
				err = fmt.Errorf("failed to rename temp db file: %w", err)
				span.RecordError(err)
				return err
			}
		}
	}
}
