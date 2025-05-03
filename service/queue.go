package service

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"queue/config"
	"queue/model"
	topicServ "queue/service/topic"
	"queue/storage"
	"queue/storage/errors"
	metadataStorage "queue/storage/metadata"
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

	mdStorage      storage.MetadataStorage
	topicService   TopicService
	messageService MessageService
}

func NewQueue(
	pCtx context.Context,
) (*Queue, error) {
	conf := config.Conf
	raftConfig := conf.RaftConfig
	dir := filepath.Join(raftConfig.LogsDataDir, strconv.FormatUint(raftConfig.ReplicaID, 10))
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	nh, err := dragonboat.NewNodeHost(drConfig.NodeHostConfig{
		RaftAddress:    raftConfig.Addr,
		NodeHostDir:    dir,
		RTTMillisecond: 100,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create replica host: %w", err)
	}
	metadataPath := filepath.Join(conf.MetadataPath, strconv.FormatUint(raftConfig.ReplicaID, 10))
	if err := os.MkdirAll(metadataPath, 0777); err != nil {
		return nil, fmt.Errorf("failed to create metadata path: %w", err)
	}
	mdStorage := metadataStorage.NewBolt(filepath.Join(metadataPath, "metadata.db"))
	if err := mdStorage.Open(pCtx); err != nil {
		return nil, fmt.Errorf("failed to open metadata storage: %w", err)
	}
	broker := &model.Broker{
		ID: raftConfig.ReplicaID,
	}
	broker.SetNodeHost(nh)
	broker.SetBrokerShardId(brokerSharID)
	q := &Queue{
		broker:       broker,
		mdStorage:    mdStorage,
		topicService: topicServ.NewDefaultTopicService(mdStorage),
	}
	factory := func(shardID uint64, replicaID uint64) statemachine.IOnDiskStateMachine {
		return NewBrokerFSM(shardID, replicaID, broker, mdStorage)
	}
	err = nh.StartOnDiskReplica(raftConfig.InviteMembers, false, factory, drConfig.Config{
		ReplicaID:          raftConfig.ReplicaID,
		ShardID:            brokerSharID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    raftConfig.Metadata.SnapshotEntries,
		CompactionOverhead: raftConfig.Metadata.CompactionOverhead,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start replica: %w", err)
	}
	if err := q.blockTillLeaderSet(pCtx, brokerSharID); err != nil {
		return nil, fmt.Errorf("block till leader set: %w", err)
	}
	if err := q.reShardExistingPartitions(pCtx); err != nil {
		return nil, fmt.Errorf("failed to re shard existing partitions: %w", err)
	}
	stop, stopCancel := context.WithCancel(pCtx)
	q.onBrokerLeaderChange(pCtx, func(prevLeaderID, currentLeaderID uint64) error {
		if prevLeaderID == broker.ID && currentLeaderID != broker.ID {
			stopCancel()
			stop, stopCancel = context.WithCancel(pCtx)
		} else if prevLeaderID != broker.ID && currentLeaderID == broker.ID {
			q.disconnectInActiveConsumers(pCtx, stop)
		}
		return nil
	})
	return q, nil
}

func (q *Queue) Close(ctx context.Context) error {
	if err := q.mdStorage.Close(ctx); err != nil {
		return fmt.Errorf("failed to close metadata storage: %w", err)
	}
	q.broker.NodeHost().Close()
	return nil
}

func (q *Queue) CreateTopic(
	pCtx context.Context,
	name string,
	numberOfPartitions uint64,
	replicationFactor uint64,
) (*model.Topic, error) {
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
		brokers := util.Sample(util.Keys(membership.Nodes), min(int(replicationFactor), len(membership.Nodes)))
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

func (q *Queue) disconnectInActiveConsumers(pCtx context.Context, stopCtx context.Context) {
	go func() {
		ticker := time.NewTicker(config.Conf.ConsumerHealthCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCtx.Done():
				return
			case <-ticker.C:
				cmd := Cmd{
					CommandType: ConsumerCommands.Consumers,
				}
				cmdBytes, err := json.Marshal(cmd)
				if err != nil {
					log.Println("failed to marshal cmd: ", err)
					return
				}
				nh := q.broker.NodeHost()
				ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
				res, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
				cancelFunc()
				if err != nil {
					log.Println("failed to propose health check: ", err)
					return
				}
				consumers := make([]*model.Consumer, 0)
				if err := json.Unmarshal(res.([]byte), &consumers); err != nil {
					log.Println("failed to unmarshal consumers: ", err)
					return
				}
				for _, consumer := range consumers {
					if consumer.LastHealthCheckAt < time.Now().Add(-config.Conf.ConsumerLostTime).Unix() {
						if err := q.disconnect(pCtx, consumer.ID); err != nil {
							log.Println("failed to disconnect consumer: ", consumer.ID)
						} else {
							log.Println("disconnected consumer: ", consumer.ID)
						}
					}
				}
			}
		}
	}()
}

func (q *Queue) SendMessage(
	pCtx context.Context,
	msg *model.Message,
) (*model.Message, error) {
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
	log.Println("selected shard", shardID, " for partition ", partitionID)
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

func (q *Queue) Connect(
	pCtx context.Context,
	consumerID, consumerGroupID string,
	topics []string,
) (*model.Consumer, *model.ConsumerGroup, error) {
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	topicsBytes, err := json.Marshal(topics)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal topics: %w", err)
	}
	cmd := Cmd{
		CommandType: ConsumerCommands.Connect,
		Args: [][]byte{
			[]byte(consumerGroupID),
			[]byte(consumerID),
			topicsBytes,
		},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal cmd: %w", err)
	}
	res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("propose connect consumer: %w", err)
	}
	var result struct {
		Consumer *model.Consumer
		Group    *model.ConsumerGroup
	}
	if err := json.Unmarshal(res.Data, &result); err != nil {
		return nil, nil, fmt.Errorf("un marshall result: %w", err)
	}
	return result.Consumer, result.Group, nil
}

func (q *Queue) disconnect(
	pCtx context.Context,
	consumerID string,
) error {
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()

	cmd := Cmd{
		CommandType: ConsumerCommands.Disconnected,
		Args: [][]byte{
			[]byte(consumerID),
		},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal cmd: %w", err)
	}
	_, err = nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return fmt.Errorf("propose disconnect consumer: %w", err)
	}
	return nil
}

func (q *Queue) ReceiveMessage(
	pCtx context.Context,
	consumerID string,
) (*model.Message, error) {
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	cmd := Cmd{
		CommandType: ConsumerCommands.ConsumerForID,
		Args: [][]byte{
			[]byte(consumerID),
		},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	res, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("propose get consumer for ID: %w", err)
	}
	var consumer model.Consumer
	if err := json.Unmarshal(res.([]byte), &consumer); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	var msg model.Message
	for range len(consumer.Partitions) {
		partitionId := consumer.GetCurrentPartition()
		shardID, ok := q.broker.ShardForPartition(partitionId)
		if !ok {
			return nil, fmt.Errorf("broker does not have partition: %s", partitionId)
		}
		log.Println("consumer polling shardID", shardID, " for partition ID ", partitionId)
		cmd = Cmd{
			CommandType: MessageCommands.Poll,
			Args: [][]byte{
				[]byte(consumer.ConsumerGroup),
				[]byte(partitionId),
			},
		}
		cmdBytes, err = json.Marshal(cmd)
		if err != nil {
			return nil, fmt.Errorf("marshal cmd: %w", err)
		}
		result, err := nh.SyncRead(ctx, shardID, cmdBytes)
		if err != nil {
			return nil, fmt.Errorf("sync read get message: %w", err)
		}
		if result == nil {
			// no message available
			consumer.IncPartitionIndex()
			continue
		}
		if err := json.Unmarshal(result.([]byte), &msg); err != nil {
			return nil, fmt.Errorf("un marshall result: %w", err)
		}
		break
	}
	consumerBytes, err := json.Marshal(consumer)
	if err != nil {
		return nil, fmt.Errorf("marshal consumer: %w", err)
	}
	cmd = Cmd{
		CommandType: ConsumerCommands.UpdateConsumer,
		Args: [][]byte{
			consumerBytes,
		},
	}
	cmdBytes, err = json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc = context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	_, err = nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("propose update consumer: %w", err)
	}
	if msg.ID == nil {
		return nil, nil
	}
	return &msg, nil
}

func (q *Queue) AckMessage(
	pCtx context.Context,
	consumerID string,
	msg *model.Message,
) error {
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	cmd := Cmd{
		CommandType: ConsumerCommands.ConsumerForID,
		Args: [][]byte{
			[]byte(consumerID),
		},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal cmd: %w", err)
	}
	res, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		return fmt.Errorf("propose get consumer for ID: %w", err)
	}
	var consumer model.Consumer
	if err := json.Unmarshal(res.([]byte), &consumer); err != nil {
		return fmt.Errorf("un marshall result: %w", err)
	}
	shardID, ok := q.broker.ShardForPartition(msg.PartitionID)
	if !ok {
		return fmt.Errorf("broker does not have partition: %s", msg.PartitionID)
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	log.Println("sending ACK command to shardID", shardID, " for partition ID ", msg.PartitionID)
	cmd = Cmd{
		CommandType: MessageCommands.Ack,
		Args: [][]byte{
			[]byte(consumer.ConsumerGroup),
			msgBytes,
		},
	}
	cmdBytes, err = json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal cmd: %w", err)
	}
	_, err = nh.SyncPropose(ctx, nh.GetNoOPSession(shardID), cmdBytes)
	if err != nil {
		return fmt.Errorf("sync read get message: %w", err)
	}
	return nil
}

func (q *Queue) HealthCheck(
	pCtx context.Context,
	consumerID string,
	healthCheckAt time.Time,
) (*model.Consumer, error) {
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	cmd := Cmd{
		CommandType: ConsumerCommands.HealthCheck,
		Args: [][]byte{
			[]byte(consumerID),
			[]byte(strconv.FormatInt(healthCheckAt.Unix(), 10)),
		},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("propose health check: %w", err)
	}
	var consumer model.Consumer
	if err := json.Unmarshal(res.Data, &consumer); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
	}
	return &consumer, nil
}

func (q *Queue) blockTillLeaderSet(
	pCtx context.Context,
	shardID uint64,
) error {
	ctx, cancelFunc := context.WithTimeout(pCtx, config.Conf.ShardLeaderWaitTime)
	ticker := time.NewTicker(config.Conf.ShardLeaderSetReCheckInterval)
	defer ticker.Stop()
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

func (q *Queue) onBrokerLeaderChange(
	pCtx context.Context,
	onChange func(prevLeaderID, currentLeaderID uint64) error,
) {
	go func() {
		var prevLeaderID uint64
		nh := q.broker.NodeHost()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pCtx.Done():
				return
			case <-ticker.C:
				leaderID, _, valid, err := nh.GetLeaderID(q.broker.BrokerShardId())
				if err != nil {
					log.Println("failed to get leader ID: ", err)
					return
				}
				if valid {
					if prevLeaderID != leaderID {
						if err := onChange(prevLeaderID, leaderID); err != nil {
							log.Println("failed to handle leader change: ", err)
						}
						prevLeaderID = leaderID
					}
				}
			}
		}
	}()
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
		q.broker.AddShardIDForPartitionID(partition.ID, shardID)
		log.Println(
			"Starting partition: ",
			partition.ID,
			" with shardID: ",
			shardID,
			"replicaID: ",
			q.broker.ID,
		)
		raftConfig := config.Conf.RaftConfig
		if err := nh.StartOnDiskReplica(
			partition.Members,
			false,
			func(shardID, replicdID uint64) statemachine.IOnDiskStateMachine {
				return NewMessageFSM(shardID, replicdID, q.broker, q.mdStorage)
			},
			drConfig.Config{
				ReplicaID:          q.broker.ID,
				ShardID:            shardID,
				ElectionRTT:        10,
				HeartbeatRTT:       1,
				CheckQuorum:        true,
				SnapshotEntries:    raftConfig.Messages.SnapshotsEntries,
				CompactionOverhead: raftConfig.Messages.CompactionOverhead,
			},
		); err != nil {
			return fmt.Errorf("failed to start replica: %w", err)
		}
		if err := q.blockTillLeaderSet(pCtx, shardID); err != nil {
			return fmt.Errorf("block till leader set: %w, for shardID: %d", err, shardID)
		}
	}

	return nil
}
