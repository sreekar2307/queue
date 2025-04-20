package embedded

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"queue/model"
	"queue/service"
	"queue/storage"
	"queue/storage/metadata"
	"queue/util"
	"strconv"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
)

const (
	brokerSharID = 1
)

type Transport struct {
	broker *model.Broker

	mu                 sync.RWMutex
	partitionToShardID map[string]uint64
	mdStorage          storage.MetadataStorage
}

func NewTransport(
	ctx context.Context,
	nodeAddr string,
	replicaID uint64,
	inviteMembers map[uint64]string,
	raftLogsDataDir string,
) (*Transport, error) {
	dir := filepath.Join(raftLogsDataDir, strconv.FormatUint(replicaID, 10))
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	nh, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		RaftAddress:    nodeAddr,
		NodeHostDir:    dir,
		RTTMillisecond: 100,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create replica host: %w", err)
	}
	if err := os.MkdirAll(filepath.Join("metadata", strconv.FormatUint(replicaID, 10)), 0777); err != nil {
		return nil, fmt.Errorf("failed to create metadata path: %w", err)
	}
	mdStorage := metadata.NewBolt(filepath.Join("metadata", strconv.FormatUint(replicaID, 10), "metadata.db"))
	if err := mdStorage.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open metadata storage: %w", err)
	}
	broker := &model.Broker{
		ID: replicaID,
	}
	broker.SetNodeHost(nh)
	broker.SetBrokerShardId(brokerSharID)
	e := &Transport{
		broker:             broker,
		partitionToShardID: make(map[string]uint64),
		mdStorage:          mdStorage,
	}
	err = nh.StartOnDiskReplica(inviteMembers, false, service.NewBrokerFSM(broker, mdStorage), config.Config{
		ReplicaID:          replicaID,
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

func (t *Transport) Connect(ctx context.Context, consumerID string, consumerGroup string) error {
	// TODO implement me
	panic("implement me")
}

func (t *Transport) Close(ctx context.Context) error {
	if err := t.mdStorage.Close(ctx); err != nil {
		return fmt.Errorf("failed to close metadata storage: %w", err)
	}
	t.broker.NodeHost().Close()
	return nil
}

func (t *Transport) CreateTopic(rCtx context.Context, name string, numberOfPartitions uint64) (*model.Topic, error) {
	nh := t.broker.NodeHost()
	cmd := service.Cmd{
		CommandType: service.TopicCommands.Create,
		Args:        [][]byte{[]byte(name), []byte(strconv.FormatUint(numberOfPartitions, 10))},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc := context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(t.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("propose create topic: %w", err)
	}
	var topic model.Topic
	if err := json.Unmarshal(res.Data, &topic); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	// get all the partitions of the topic create a shard per partition, randomly add 3 nodes per partition
	cmd = service.Cmd{
		CommandType: service.PartitionsCommands.GetPartitions,
		Args:        [][]byte{[]byte(topic.Name)},
	}
	cmdBytes, err = json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	numPartitionsRes, err := nh.SyncRead(ctx, t.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("sync read get partitions: %w", err)
	}
	partitions := make([]*model.Partition, 0)
	if err := json.Unmarshal(numPartitionsRes.([]byte), &partitions); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	membership, err := nh.SyncGetShardMembership(ctx, t.broker.BrokerShardId())
	if err != nil {
		return nil, fmt.Errorf("get shard membership: %w", err)
	}
	for _, partition := range partitions {
		t.mu.Lock()
		t.partitionToShardID[partition.ID] = uint64(len(t.partitionToShardID)) + brokerSharID + 1
		shardID := t.partitionToShardID[partition.ID]
		t.mu.Unlock()
		brokers := util.Sample(util.Keys(membership.Nodes), min(3, len(membership.Nodes)))
		brokerTargets := make(map[uint64]string)
		for _, broker := range brokers {
			brokerTargets[broker] = membership.Nodes[broker]
		}
		members, err := json.Marshal(brokerTargets)
		if err != nil {
			return nil, fmt.Errorf("marshal members: %w", err)
		}
		cmd = service.Cmd{
			CommandType: service.PartitionsCommands.PartitionAdded,
			Args: [][]byte{
				[]byte(strconv.FormatUint(shardID, 10)),
				members,
			},
		}
		cmdBytes, err = json.Marshal(cmd)
		ctx, cancelFunc = context.WithTimeout(rCtx, 15*time.Second)
		res, err = nh.SyncPropose(ctx, nh.GetNoOPSession(t.broker.BrokerShardId()), cmdBytes)
		cancelFunc()
		if err != nil {
			return nil, fmt.Errorf("propose partition added: %w", err)
		}
		if err := t.blockTillLeaderSet(rCtx, shardID); err != nil {
			return nil, err
		}
	}

	return &topic, nil
}

func (t *Transport) SendMessage(rCtx context.Context, msg *model.Message) (*model.Message, error) {
	nh := t.broker.NodeHost()
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	cmd := service.Cmd{
		CommandType: service.PartitionsCommands.PartitionID,
		Args:        [][]byte{msgBytes},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc := context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	partitionIDBytes, err := nh.SyncRead(ctx, t.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("sync read partitionID: %w", err)
	}
	msg.PartitionID = string(partitionIDBytes.([]byte))
	msgBytes, err = json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	cmd = service.Cmd{
		CommandType: service.MessageCommands.Append,
		Args:        [][]byte{msgBytes},
	}
	cmdBytes, err = json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(rCtx, 15*time.Second)
	defer cancelFunc()
	t.mu.RLock()
	shardID := t.partitionToShardID[msg.PartitionID]
	t.mu.RUnlock()
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

func (t *Transport) ReceiveMessage(ctx context.Context, topic, consumerGroup string) (*model.Message, error) {
	// TODO implement me
	panic("implement me")
}

func (t *Transport) AckMessage(ctx context.Context, topic, consumerGroup string, message *model.Message) error {
	// TODO implement me
	panic("implement me")
}

func (t *Transport) blockTillLeaderSet(pCtx context.Context, shardID uint64) error {
	ctx, cancelFunc := context.WithTimeout(pCtx, 30*time.Hour)
	defer cancelFunc()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for leader to be set")
		default:
			_, _, ok, err := t.broker.NodeHost().GetLeaderID(shardID)
			if err != nil {
				return fmt.Errorf("leader for broker shard: %w", err)
			}
			if ok {
				return nil
			}
		}
	}
}
