package message

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/storage"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"io"
	"log"
	"os"
	"sync"
)

type DefaultMessageService struct {
	messageStorage storage.MessageStorage
	// Only read operations are allowed on metadata storage when using from message service
	metadataStorage storage.MetadataStorage

	partitionsMutex sync.RWMutex
	partitionsLocks map[string]*sync.Mutex
	partitionsPath  string
	broker          *model.Broker
}

func NewDefaultMessageService(
	messageStorage storage.MessageStorage,
	metadata storage.MetadataStorage,
	partitionsPath string,
	broker *model.Broker,
) *DefaultMessageService {
	return &DefaultMessageService{
		messageStorage:  messageStorage,
		metadataStorage: metadata,
		partitionsLocks: make(map[string]*sync.Mutex),
		partitionsPath:  partitionsPath,
		broker:          broker,
	}
}

var (
	ErrNoNewMessages       = errors.New("no new messages")
	ErrRebalanceInProgress = errors.New("rebalance in progress")
	ErrDuplicateCommand    = errors.New("duplicate command")
)

func (d *DefaultMessageService) LastAppliedCommandID(ctx context.Context, shardID uint64) (uint64, error) {
	partitionIDs := make([]string, 0)
	partitions, err := d.metadataStorage.AllPartitions(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get partitions: %w", err)
	}
	for _, partition := range partitions {
		if partition.ShardID == shardID {
			partitionIDs = append(partitionIDs, partition.ID)
		}
	}
	lastAppliedCommandID, err := d.messageStorage.LastAppliedCommandID(ctx, partitionIDs)
	if err != nil {
		return 0, fmt.Errorf("failed to get last applied command ID: %w", err)
	}
	return lastAppliedCommandID, nil
}

func (d *DefaultMessageService) AppendMessage(ctx context.Context, commandID uint64, message *model.Message) error {
	if len(message.Data) == 0 {
		return fmt.Errorf("message data is empty")
	}
	msgID, err := d.nextMessageID(ctx, message.PartitionID)
	if err != nil {
		return fmt.Errorf("failed to get next msgID: %w", err)
	}
	message.ID = msgID
	if err := d.messageStorage.AppendMessage(ctx, commandID, message); err != nil {
		if errors.Is(err, storageErrors.ErrDuplicateCommand) {
			return errors.Join(err, ErrDuplicateCommand)
		}
		return fmt.Errorf("failed to append message: %w", err)
	}
	if message.ID == nil {
		return fmt.Errorf("message ID is empty: expected to be set by storage")
	}
	return nil
}

func (d *DefaultMessageService) Poll(
	ctx context.Context,
	consumerGroupID string,
	partitionID string,
) (_ *model.Message, err error) {
	//consumer, err := d.metadataStorage.Consumer(ctx, consumerID)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to get consumer: %w", err)
	//}
	consumerGroup, err := d.metadataStorage.ConsumerGroup(ctx, consumerGroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group: %w", err)
	}
	if consumerGroup.RebalanceInProgress() {
		return nil, ErrRebalanceInProgress
	}

	selectedPartition, err := d.metadataStorage.Partition(ctx, partitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition: %w", err)
	}
	msgId, err := d.messageStorage.NextUnAckedMessageID(ctx, selectedPartition, consumerGroup)
	if err != nil {
		if errors.Is(err, storageErrors.ErrNoMessageFound) {
			return nil, errors.Join(err, ErrNoNewMessages)
		}
		return nil, fmt.Errorf("failed to get next message ID: %w", err)
	}
	log.Println("receiving  messageID ", msgId, " from partition ", partitionID)
	message, err := d.messageStorage.MessageAtIndex(ctx, selectedPartition, msgId)
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}
	return message, nil
}

func (d *DefaultMessageService) AckMessage(
	ctx context.Context,
	commandID uint64,
	consumerGroupID string,
	message *model.Message,
) (err error) {
	consumerGroup, err := d.metadataStorage.ConsumerGroup(ctx, consumerGroupID)
	if err != nil {
		return fmt.Errorf("failed to get consumer group: %w", err)
	}
	if consumerGroup.RebalanceInProgress() {
		return ErrRebalanceInProgress
	}
	err = d.messageStorage.AckMessage(ctx, commandID, message, consumerGroup)
	if err != nil {
		if errors.Is(err, storageErrors.ErrDuplicateCommand) {
			return errors.Join(err, ErrDuplicateCommand)
		}
		return fmt.Errorf("failed to ack message: %w", err)
	}
	return nil
}

func (d *DefaultMessageService) Close(ctx context.Context) error {
	if err := d.messageStorage.Close(ctx); err != nil {
		return fmt.Errorf("failed to close message storage: %w", err)
	}
	return nil
}

func (d *DefaultMessageService) Open(_ context.Context) error {
	if err := os.MkdirAll(d.partitionsPath, 0777); err != nil {
		return fmt.Errorf("create partitions path: %w", err)
	}
	return nil
}

func (d *DefaultMessageService) Snapshot(ctx context.Context, writer io.Writer) error {
	log.Println("taking snapshot of message storage")
	if err := d.messageStorage.Snapshot(ctx, writer); err != nil {
		return fmt.Errorf("failed to snapshot message storage: %w", err)
	}
	log.Println("snapshot of message storage taken")
	return nil
}

func (d *DefaultMessageService) RecoverFromSnapshot(ctx context.Context, reader io.Reader) error {
	if err := d.messageStorage.RecoverFromSnapshot(ctx, reader); err != nil {
		return fmt.Errorf("failed to recover message storage: %w", err)
	}
	return nil
}

func (d *DefaultMessageService) nextMessageID(ctx context.Context, partitionKey string) ([]byte, error) {
	d.partitionsMutex.RLock()
	mu, ok := d.partitionsLocks[partitionKey]
	d.partitionsMutex.RUnlock()
	if !ok {
		d.partitionsMutex.Lock()
		// double check
		if mu, ok = d.partitionsLocks[partitionKey]; !ok {
			mu = &sync.Mutex{}
			d.partitionsLocks[partitionKey] = mu
		}
		d.partitionsMutex.Unlock()
	}
	mu.Lock()
	defer mu.Unlock()
	lastMsgID, err := d.messageStorage.LastMessageID(ctx, partitionKey)
	if err != nil {
		return nil, err
	}
	nextMsgID := make([]byte, 8)
	if lastMsgID == nil {
		binary.BigEndian.PutUint64(nextMsgID, 1)
	} else {
		binary.BigEndian.PutUint64(nextMsgID, binary.BigEndian.Uint64(lastMsgID)+1)
	}
	return nextMsgID, nil
}
