package service

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"queue/model"
	"queue/service/topic"
	"queue/storage"
	"queue/storage/errors"
	"queue/storage/metadata"
	"queue/util"
	"strconv"
	"time"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/lni/dragonboat/v4"
	drConfig "github.com/lni/dragonboat/v4/config"
)

const (
	brokerSharID = 1
)

type Queue struct {
	broker *model.Broker

	mdStorage    storage.MetadataStorage
	config       Config
	topicService TopicService
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
		broker:       broker,
		mdStorage:    mdStorage,
		config:       config,
		topicService: topic.NewDefaultTopicService(mdStorage),
	}
	factory := func(shardID uint64, replicaID uint64) statemachine.IOnDiskStateMachine {
		if shardID == brokerSharID {
			return NewBrokerFSM(shardID, replicaID, config, broker, mdStorage)
		}
		return NewMessageFSM(shardID, replicaID, config, broker, mdStorage)
	}
	err = nh.StartOnDiskReplica(config.InviteMembers, false, factory, drConfig.Config{
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
		return nil, fmt.Errorf("block till leader set: %w", err)
	}
	if err := e.reShardExistingPartitions(ctx); err != nil {
		return nil, fmt.Errorf("failed to re shard existing partitions: %w", err)
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

func (q *Queue) CreateTopic(pCtx context.Context, name string, numberOfPartitions uint64) (*model.Topic, error) {
	nh := q.broker.NodeHost()
	cmd := Cmd{
		CommandType: TopicCommands.CreateTopic,
		Args: [][]byte{
			[]byte(name),
			[]byte(strconv.FormatUint(numberOfPartitions, 10)),
			[]byte(strconv.FormatUint(brokerSharID+1, 10)),
		},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("propose create topic: %w", err)
	}
	if binary.BigEndian.Uint64(res.Data[:8]) == 0 {
		return nil, errors.ErrTopicAlreadyExists
	}
	var topic model.Topic
	if err := json.Unmarshal(res.Data[8:], &topic); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	// get all the partitions of the topic create a shard per partition, randomly add 3 nodes per partition
	cmd = Cmd{
		CommandType: PartitionsCommands.PartitionsForTopic,
		Args:        [][]byte{[]byte(topic.Name)},
	}
	cmdBytes, err = json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	numPartitionsRes, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("sync read get partitions: %w", err)
	}
	partitions := make([]*model.Partition, 0)
	if err := json.Unmarshal(numPartitionsRes.([]byte), &partitions); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	membership, err := nh.SyncGetShardMembership(ctx, q.broker.BrokerShardId())
	if err != nil {
		return nil, fmt.Errorf("get shard membership: %w", err)
	}
	for _, partition := range partitions {
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
				[]byte(partition.ID),
				[]byte(strconv.FormatUint(partition.ShardID, 10)),
				members,
			},
		}
		cmdBytes, err = json.Marshal(cmd)
		ctx, cancelFunc = context.WithTimeout(pCtx, 15*time.Second)
		res, err = nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
		cancelFunc()
		if err != nil {
			return nil, fmt.Errorf("propose partition added: %w", err)
		}
		if err := q.blockTillLeaderSet(pCtx, partition.ShardID); err != nil {
			return nil, err
		}
	}

	return &topic, nil
}

func (q *Queue) SendMessage(pCtx context.Context, msg *model.Message) (*model.Message, error) {
	nh := q.broker.NodeHost()
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	partitionID, err := q.topicService.PartitionID(pCtx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitionID: %w", err)
	}
	partition, err := q.topicService.GetPartition(pCtx, partitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition: %w", err)
	}
	msg.PartitionID = partitionID
	msgBytes, err = json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	shardID, ok := q.broker.ShardForPartition(partitionID)
	if !ok {
		return nil, fmt.Errorf("failed to get shardID for partition: %w", err)
	}
	if partition.ShardID != shardID {
		return nil, fmt.Errorf("shardID mismatch: %d != %d", partition.ShardID, shardID)
	}
	cmd := Cmd{
		CommandType: MessageCommands.Append,
		Args:        [][]byte{msgBytes},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(partition.ShardID), cmdBytes)
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

func (q *Queue) reShardExistingPartitions(pCtx context.Context) error {
	nh := q.broker.NodeHost()
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	cmd := Cmd{
		CommandType: PartitionsCommands.Partitions,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal cmd: %w", err)
	}
	result, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		return fmt.Errorf("sync read all partitions: %w", err)
	}
	partitions := make([]*model.Partition, 0)
	if err := json.Unmarshal(result.([]byte), &partitions); err != nil {
		return fmt.Errorf("un marshall result: %w", err)
	}
	for _, partition := range partitions {
		if partition.ShardID == 0 || len(partition.Members) == 0 {
			continue
		}
		shardID := partition.ShardID
		if _, ok := partition.Members[q.broker.ID]; !ok {
			// current broker is not a member of the partition
			continue
		}
		q.broker.AddPartitionShards(partition.ID, shardID)
		if !nh.HasNodeInfo(shardID, q.broker.ID) {
			log.Println(
				"Starting partition: ",
				partition.ID,
				" with shardID: ",
				shardID,
				"replicaID: ",
				q.broker.ID,
			)

			if err := nh.StartOnDiskReplica(
				partition.Members,
				false,
				func(shardID, replicdID uint64) statemachine.IOnDiskStateMachine {
					return NewMessageFSM(shardID, replicdID, q.config, q.broker, q.mdStorage)
				},
				drConfig.Config{
					ReplicaID:          q.broker.ID,
					ShardID:            shardID,
					ElectionRTT:        10,
					HeartbeatRTT:       1,
					CheckQuorum:        true,
					SnapshotEntries:    1000,
					CompactionOverhead: 50,
				},
			); err != nil {
				return fmt.Errorf("failed to start replica: %w", err)
			}
		}

		if err := q.blockTillLeaderSet(pCtx, shardID); err != nil {
			return fmt.Errorf("block till leader set: %w, for shardID: %d", err, shardID)
		}
	}

	return nil
}
