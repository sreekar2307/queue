package message

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"queue/model"
	"queue/storage"
	"queue/util"
	"sync"
)

type DefaultMessageService struct {
	MessageStorage  storage.MessageStorage
	MetadataStorage storage.MetadataStorage

	partitionsMutex sync.RWMutex
	partitionsLocks map[string]*sync.Mutex
	partitionsPath  string
}

func NewDefaultMessageService(
	messageStorage storage.MessageStorage,
	metadata storage.MetadataStorage,
	partitionsPath string,
) *DefaultMessageService {
	return &DefaultMessageService{
		MessageStorage:  messageStorage,
		MetadataStorage: metadata,
		partitionsLocks: make(map[string]*sync.Mutex),
		partitionsPath:  partitionsPath,
	}
}

var (
	NoNewMessages       = errors.New("no new messages")
	RebalanceInProgress = errors.New("rebalance in progress")
)

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
	lastMsgID, err := d.MessageStorage.LastMessageID(ctx, partitionKey)
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

func (d *DefaultMessageService) AppendMessage(ctx context.Context, message *model.Message) error {
	if len(message.Data) == 0 {
		return fmt.Errorf("message data is empty")
	}
	msgID, err := d.nextMessageID(ctx, message.PartitionID)
	if err != nil {
		return fmt.Errorf("failed to get next msgID: %w", err)
	}
	message.ID = msgID
	if err := d.MessageStorage.AppendMessage(ctx, message); err != nil {
		return fmt.Errorf("failed to append message: %w", err)
	}
	if message.ID == nil {
		return fmt.Errorf("message ID is empty: expected to be set by storage")
	}
	return nil
}

func (d *DefaultMessageService) Poll(
	ctx context.Context,
	consumerBrokerID string,
) (_ *model.Message, err error) {
	consumer, err := d.MetadataStorage.Consumer(ctx, consumerBrokerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer: %w", err)
	}
	consumerGroup, err := d.MetadataStorage.ConsumerGroup(ctx, consumer.ConsumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group: %w", err)
	}
	if consumerGroup.RebalanceInProgress() {
		return nil, RebalanceInProgress
	}
	assignedPartitions := consumer.Partitions
	if len(assignedPartitions) == 0 {
		return nil, fmt.Errorf("no assigned partitions for consumer %s", consumer.ID)
	}
	var (
		selectedPartition *model.Partition
		msgId             []byte
	)

	for range assignedPartitions { // to avoid infinite loop in buggy conditions
		paritionIndex := consumer.PartitionIndex()

		selectedPartition, err = d.MetadataStorage.Partition(ctx, assignedPartitions[paritionIndex])
		if err != nil {
			return nil, fmt.Errorf("failed to get partition: %w", err)
		}
		consumer.IncPartitionIndex()
		msgId, err = d.MessageStorage.NextUnAckedMessageID(ctx, selectedPartition, consumerGroup)
		if err != nil {
			if errors.Is(err, NoNewMessages) {
				continue
			}
			return nil, fmt.Errorf("failed to get next message ID: %w", err)
		}
		break
	}
	err = d.MetadataStorage.UpdateConsumer(ctx, consumer)
	if err != nil {
		return nil, fmt.Errorf("failed to updated consumer partition index: %w", err)
	}
	if msgId == nil {
		return nil, NoNewMessages
	}
	message, err := d.MessageStorage.MessageAtIndex(ctx, selectedPartition, msgId)
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}
	return message, nil
}

func (d *DefaultMessageService) AckMessage(
	ctx context.Context,
	consumerBrokerID string,
	message *model.Message,
) (err error) {
	consumer, err := d.MetadataStorage.Consumer(ctx, consumerBrokerID)
	if err != nil {
		return fmt.Errorf("failed to get consumer: %w", err)
	}
	consumerGroup, err := d.MetadataStorage.ConsumerGroup(ctx, consumer.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("failed to get consumer group: %w", err)
	}
	if consumerGroup.RebalanceInProgress() {
		return RebalanceInProgress
	}
	assignedPartions := consumer.Partitions
	if len(assignedPartions) == 0 {
		return fmt.Errorf("no assigned partitions for consumer %s", consumer.ID)
	}
	_, ok := util.FirstMatch(assignedPartions, func(partitionID string) bool {
		return message.PartitionID == partitionID
	})
	if !ok {
		return fmt.Errorf("message %s is not assigned to consumer %s", message.ID, consumer.ID)
	}
	err = d.MessageStorage.AckMessage(ctx, message, consumerGroup)
	if err != nil {
		return fmt.Errorf("failed to ack message: %w", err)
	}
	return nil
}

func (d *DefaultMessageService) Close(ctx context.Context) error {
	if err := d.MessageStorage.Close(ctx); err != nil {
		return fmt.Errorf("failed to close message storage: %w", err)
	}
	return nil
}

func (d *DefaultMessageService) Open(ctx context.Context) error {
	if err := os.MkdirAll(d.partitionsPath, 0777); err != nil {
		return fmt.Errorf("create partitions path: %w", err)
	}
	return nil
}

func (d *DefaultMessageService) Snapshot(ctx context.Context, writer io.Writer) error {
	if err := d.MessageStorage.Snapshot(ctx, writer); err != nil {
		return fmt.Errorf("failed to snapshot message storage: %w", err)
	}
	return nil
}

func (d *DefaultMessageService) RecoverFromSnapshot(ctx context.Context, reader io.Reader) error {
	if err := d.MessageStorage.RecoverFromSnapshot(ctx, reader); err != nil {
		return fmt.Errorf("failed to recover message storage: %w", err)
	}
	return nil
}
