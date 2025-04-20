package message

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"queue/model"
	"slices"
	"sync"

	boltDB "go.etcd.io/bbolt"
)

type Bolt struct {
	PartitionsPath string
	mu             sync.RWMutex
	dbs            map[string]*boltDB.DB
}

func NewBolt(partitionPath string) *Bolt {
	return &Bolt{
		PartitionsPath: partitionPath,
		dbs:            make(map[string]*boltDB.DB),
	}
}

const (
	messagesBucket               = "messages"
	messageStatsBucket           = "messages_stats"
	messageStatsPartitionsBucket = "partitions"
)

func (b *Bolt) Close(context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, db := range b.dbs {
		if err := db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}
	return nil
}

func (b *Bolt) AppendMessage(_ context.Context, message *model.Message) error {
	db, err := b.getDBForPartition(message.PartitionID)
	if err != nil {
		return fmt.Errorf("failed to get database for partition: %w", err)
	}
	if len(message.ID) == 0 {
		return fmt.Errorf("message id is not set")
	}
	return db.Update(func(tx *boltDB.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(messagesBucket))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		messageData, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		if err := bucket.Put(message.ID, slices.Clone(messageData)); err != nil {
			return fmt.Errorf("failed to put message: %w", err)
		}
		return nil
	})
}

func (b *Bolt) MessageAtIndex(_ context.Context, partition *model.Partition, messageID []byte) (*model.Message, error) {
	db, err := b.getDBForPartition(partition.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database for partition: %w", err)
	}
	var message model.Message
	err = db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(messagesBucket))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		data := bucket.Get(messageID)
		if data == nil {
			return fmt.Errorf("message not found")
		}
		if err := json.Unmarshal(data, &message); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}
	return &message, nil
}

func (b *Bolt) getDBForPartition(partitionKey string) (*boltDB.DB, error) {
	b.mu.RLock()
	db, ok := b.dbs[partitionKey]
	b.mu.RUnlock()
	if ok {
		return db, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Double check
	if db, ok := b.dbs[partitionKey]; ok {
		return db, nil
	}
	newDB, err := boltDB.Open(filepath.Join(b.PartitionsPath, partitionKey), 0777, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	b.dbs[partitionKey] = newDB
	return newDB, nil
}

func (b *Bolt) AckMessage(_ context.Context, message *model.Message, group *model.ConsumerGroup) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	db, err := b.getDBForPartition(message.PartitionID)
	if err != nil {
		return fmt.Errorf("failed to get database for partition: %w", err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	bucket, err := tx.CreateBucketIfNotExists([]byte(messageStatsBucket))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	partitionBucket, err := bucket.CreateBucketIfNotExists([]byte(messageStatsPartitionsBucket))
	if err != nil {
		return fmt.Errorf("failed to create partition bucket: %w", err)
	}
	if err := partitionBucket.Put([]byte(group.ID), message.ID); err != nil {
		return fmt.Errorf("failed to put acked message: %w", err)
	}
	return tx.Commit()
}

func (b *Bolt) LastMessageID(_ context.Context, partitionKey string) ([]byte, error) {
	db, err := b.getDBForPartition(partitionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get database for partition: %w", err)
	}
	var lastMessageID []byte
	db.View(func(tx *boltDB.Tx) error {
		bucket := tx.Bucket([]byte(messagesBucket))
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

func (b *Bolt) NextUnAckedMessageID(_ context.Context, partition *model.Partition, group *model.ConsumerGroup) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	db, err := b.getDBForPartition(partition.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database for partition: %w", err)
	}
	var nextMesssageID []byte
	err = db.View(func(tx *boltDB.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(messageStatsBucket))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		partitionBucket, err := bucket.CreateBucketIfNotExists([]byte(messageStatsPartitionsBucket))
		if err != nil {
			return fmt.Errorf("failed to create partition bucket: %w", err)
		}
		lastAckedMsgId := partitionBucket.Get([]byte(group.ID))
		msgBucket := tx.Bucket([]byte(messagesBucket))
		if msgBucket == nil {
			return fmt.Errorf("msgBucket not found")
		}
		cursor := msgBucket.Cursor()
		if lastAckedMsgId == nil {
			nextMesssageID, _ = cursor.First()
			return nil
		}
		cursor.Seek(lastAckedMsgId)
		_, nextMesssageID = cursor.Next()
		return nil
	})
	return nextMesssageID, nil
}
