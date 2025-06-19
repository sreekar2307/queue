package message

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/sreekar2307/queue/logger"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service"
	"github.com/sreekar2307/queue/storage"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"io"
	"os"
	"sync"
)

type messageService struct {
	messageStorage storage.MessageStorage
	// Only read operations are allowed on metadata storage when using from message service
	metadataStorage storage.MetadataStorage

	partitionsLocks sync.Map
	partitionsPath  string
	broker          *model.Broker
	log             logger.Logger
}

func NewMessageService(
	messageStorage storage.MessageStorage,
	metadata storage.MetadataStorage,
	partitionsPath string,
	broker *model.Broker,
	log logger.Logger,
) service.MessageService {
	return &messageService{
		messageStorage:  messageStorage,
		metadataStorage: metadata,
		partitionsPath:  partitionsPath,
		broker:          broker,
		log:             log,
	}
}

var (
	ErrRebalanceInProgress = errors.New("rebalance in progress")
)

func (d *messageService) LastAppliedCommandID(ctx context.Context, shardID uint64) (uint64, error) {
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

func (d *messageService) AppendMessage(ctx context.Context, commandID uint64, message *model.Message) error {
	if len(message.Data) == 0 {
		return fmt.Errorf("message data is empty")
	}
	lock := d.lockForPartition(message.PartitionID)
	lock.Lock()
	defer lock.Unlock()
	msgID, err := d.nextMessageID(ctx, message.PartitionID)
	if err != nil {
		return fmt.Errorf("failed to get next msgID: %w", err)
	}
	message.ID = msgID
	if err := d.messageStorage.AppendMessage(ctx, commandID, message); err != nil {
		if errors.Is(err, storageErrors.ErrDuplicateCommand) {
			return storageErrors.ErrDuplicateCommand
		}
		return fmt.Errorf("failed to append message: %w", err)
	}
	if message.ID == nil {
		return fmt.Errorf("message ID is empty: expected to be set by storage")
	}
	return nil
}

func (d *messageService) Poll(
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
			return nil, storageErrors.ErrNoMessageFound
		}
		return nil, fmt.Errorf("failed to get next message ID: %w", err)
	}
	d.log.Info(
		ctx,
		"receiving",
		logger.NewAttr("msgID", msgId),
		logger.NewAttr("partitionID", partitionID),
		logger.NewAttr("consumerGroupID", consumerGroupID),
	)
	message, err := d.messageStorage.MessageAtIndex(ctx, selectedPartition, msgId)
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}
	return message, nil
}

func (d *messageService) AckMessage(
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
			return storageErrors.ErrDuplicateCommand
		}
		return fmt.Errorf("failed to ack message: %w", err)
	}
	return nil
}

func (d *messageService) Close(ctx context.Context) error {
	if err := d.messageStorage.Close(ctx); err != nil {
		return fmt.Errorf("failed to close message storage: %w", err)
	}
	return nil
}

func (d *messageService) Open(_ context.Context) error {
	if err := os.MkdirAll(d.partitionsPath, 0777); err != nil {
		return fmt.Errorf("create partitions path: %w", err)
	}
	return nil
}

func (d *messageService) Snapshot(ctx context.Context, writer io.Writer) error {
	if err := d.messageStorage.Snapshot(ctx, writer); err != nil {
		return fmt.Errorf("failed to snapshot message storage: %w", err)
	}
	return nil
}

func (d *messageService) RecoverFromSnapshot(ctx context.Context, reader io.Reader) error {
	if err := d.messageStorage.RecoverFromSnapshot(ctx, reader); err != nil {
		return fmt.Errorf("failed to recover message storage: %w", err)
	}
	return nil
}

// nextMessageID returns the next message ID for the given partition key. this method is expected to be called
// under a lock for the partition to ensure that the message ID is unique and sequential.
func (d *messageService) nextMessageID(ctx context.Context, partitionKey string) ([]byte, error) {
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

func (d *messageService) lockForPartition(partitionID string) *sync.Mutex {
	mu, _ := d.partitionsLocks.LoadOrStore(partitionID, new(sync.Mutex))
	return mu.(*sync.Mutex)
}
