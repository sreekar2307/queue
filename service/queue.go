package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"queue/model"
	"queue/storage"
	"queue/storage/metadata"
	"queue/util"
	"strconv"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4"
	drConfig "github.com/lni/dragonboat/v4/config"
)

const (
	brokerSharID = 1
)

type Queue struct {
	broker *model.Broker

	mdStorage storage.MetadataStorage

	mu                 sync.RWMutex
	partitionToShardID map[string]uint64
}

func NewQueue(
	ctx context.Context,
	config Config,
) (*Queue, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	dir := filepath.Join(config.RaftLogsDataDir, strconv.FormatUint(config.ReplicaID, 10))
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	nh, err := dragonboat.NewNodeHost(drConfig.NodeHostConfig{
		RaftAddress:    config.RaftNodeAddr,
		NodeHostDir:    dir,
		RTTMillisecond: 100,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create replica host: %w", err)
	}
	metadataPath := filepath.Join(config.MetadataPath, strconv.FormatUint(config.ReplicaID, 10))
	if err := os.MkdirAll(metadataPath, 0777); err != nil {
		return nil, fmt.Errorf("failed to create metadata path: %w", err)
	}
	mdStorage := metadata.NewBolt(filepath.Join(metadataPath, "metadata.db"))
	if err := mdStorage.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open metadata storage: %w", err)
	}
	broker := &model.Broker{
		ID: config.ReplicaID,
	}
	broker.SetNodeHost(nh)
	broker.SetBrokerShardId(brokerSharID)
	e := &Queue{
		broker:             broker,
		partitionToShardID: make(map[string]uint64),
		mdStorage:          mdStorage,
	}
	err = nh.StartOnDiskReplica(config.InviteMembers, false, NewBrokerFSM(config.PartitionsPath, broker, mdStorage), drConfig.Config{
		ReplicaID:          config.ReplicaID,
		ShardID:            brokerSharID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    1000,
		CompactionOverhead: 50,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start replica: %w", err)
	}
	if err := e.blockTillLeaderSet(ctx, brokerSharID); err != nil {
		return nil, err
	}
	return e, nil
}

func (q *Queue) Close(ctx context.Context) error {
	if err := q.mdStorage.Close(ctx); err != nil {
		return fmt.Errorf("failed to close metadata storage: %w", err)
	}
	q.broker.NodeHost().Close()
	return nil
}

func (q *Queue) CreateTopic(rCtx context.Context, name string, numberOfPartitions uint64) (*model.Topic, error) {
	nh := q.broker.NodeHost()
	cmd := Cmd{
		CommandType: TopicCommands.Create,
		Args:        [][]byte{[]byte(name), []byte(strconv.FormatUint(numberOfPartitions, 10))},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc := context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("propose create topic: %w", err)
	}
	var topic model.Topic
	if err := json.Unmarshal(res.Data, &topic); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	// get all the partitions of the topic create a shard per partition, randomly add 3 nodes per partition
	cmd = Cmd{
		CommandType: PartitionsCommands.GetPartitions,
		Args:        [][]byte{[]byte(topic.Name)},
	}
	cmdBytes, err = json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	numPartitionsRes, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("sync read get partitions: %w", err)
	}
	partitions := make([]*model.Partition, 0)
	if err := json.Unmarshal(numPartitionsRes.([]byte), &partitions); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	membership, err := nh.SyncGetShardMembership(ctx, q.broker.BrokerShardId())
	if err != nil {
		return nil, fmt.Errorf("get shard membership: %w", err)
	}
	for _, partition := range partitions {
		q.mu.Lock()
		q.partitionToShardID[partition.ID] = uint64(len(q.partitionToShardID)) + brokerSharID + 1
		shardID := q.partitionToShardID[partition.ID]
		q.mu.Unlock()
		brokers := util.Sample(util.Keys(membership.Nodes), min(3, len(membership.Nodes)))
		brokerTargets := make(map[uint64]string)
		for _, broker := range brokers {
			brokerTargets[broker] = membership.Nodes[broker]
		}
		members, err := json.Marshal(brokerTargets)
		if err != nil {
			return nil, fmt.Errorf("marshal members: %w", err)
		}
		cmd = Cmd{
			CommandType: PartitionsCommands.PartitionAdded,
			Args: [][]byte{
				[]byte(strconv.FormatUint(shardID, 10)),
				members,
			},
		}
		cmdBytes, err = json.Marshal(cmd)
		ctx, cancelFunc = context.WithTimeout(rCtx, 15*time.Second)
		res, err = nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
		cancelFunc()
		if err != nil {
			return nil, fmt.Errorf("propose partition added: %w", err)
		}
		if err := q.blockTillLeaderSet(rCtx, shardID); err != nil {
			return nil, err
		}
	}

	return &topic, nil
}

func (q *Queue) SendMessage(rCtx context.Context, msg *model.Message) (*model.Message, error) {
	nh := q.broker.NodeHost()
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	cmd := Cmd{
		CommandType: PartitionsCommands.PartitionID,
		Args:        [][]byte{msgBytes},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc := context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	partitionIDBytes, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("sync read partitionID: %w", err)
	}
	msg.PartitionID = string(partitionIDBytes.([]byte))
	msgBytes, err = json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	cmd = Cmd{
		CommandType: MessageCommands.Append,
		Args:        [][]byte{msgBytes},
	}
	cmdBytes, err = json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	q.mu.RLock()
	shardID := q.partitionToShardID[msg.PartitionID]
	q.mu.RUnlock()
	res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(shardID), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("propose append messages: %w", err)
	}
	var msgRes model.Message
	if err := json.Unmarshal(res.Data, &msgRes); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	return &msgRes, nil
}

func (q *Queue) blockTillLeaderSet(pCtx context.Context, shardID uint64) error {
	ctx, cancelFunc := context.WithTimeout(pCtx, 30*time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer cancelFunc()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for leader to be set")
		case <-ticker.C:
			_, _, ok, err := q.broker.NodeHost().GetLeaderID(shardID)
			if err != nil {
				return fmt.Errorf("leader for broker shard: %w", err)
			}
			if ok {
				return nil
			}
		}
	}
}
