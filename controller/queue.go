package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	brokerFSM "github.com/sreekar2307/queue/raft/fsm/broker"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"github.com/sreekar2307/queue/raft/fsm/command/factory"
	messageFSM "github.com/sreekar2307/queue/raft/fsm/message"
	"github.com/sreekar2307/queue/service"

	"github.com/lni/dragonboat/v4/raftio"

	"github.com/sreekar2307/queue/config"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service/errors"
	topicServ "github.com/sreekar2307/queue/service/topic"
	"github.com/sreekar2307/queue/storage"
	metadataStorage "github.com/sreekar2307/queue/storage/metadata"
	"github.com/sreekar2307/queue/util"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/lni/dragonboat/v4"
	drConfig "github.com/lni/dragonboat/v4/config"
	pbBrokerCommands "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
)

const (
	brokerSharID = 101
)

type Queue struct {
	broker *model.Broker

	mdStorage      storage.MetadataStorage
	topicService   service.TopicService
	messageService service.MessageService
	pCtx           context.Context

	mu                       sync.Mutex
	deactivateConsumerCancel context.CancelFunc
	prevBrokerShardLeaderID  uint64
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
	metadataPath := filepath.Join(conf.MetadataPath, strconv.FormatUint(raftConfig.ReplicaID, 10))
	if err := os.MkdirAll(metadataPath, 0777); err != nil {
		return nil, fmt.Errorf("failed to create metadata path: %w", err)
	}
	mdStorage := metadataStorage.NewBolt(filepath.Join(metadataPath, "metadata.db"))
	if err := mdStorage.Open(pCtx); err != nil {
		return nil, fmt.Errorf("failed to open metadata storage: %w", err)
	}
	broker := &model.Broker{
		ID:               raftConfig.ReplicaID,
		RaftAddress:      raftConfig.Addr,
		ReachGrpcAddress: conf.GRPC.ListenerAddr,
		ReachHttpAddress: conf.HTTP.ListenerAddr,
	}
	q := &Queue{
		broker:       broker,
		mdStorage:    mdStorage,
		topicService: topicServ.NewDefaultTopicService(mdStorage),
		pCtx:         pCtx,
	}
	nh, err := dragonboat.NewNodeHost(drConfig.NodeHostConfig{
		RaftAddress:       raftConfig.Addr,
		NodeHostDir:       dir,
		RTTMillisecond:    100,
		RaftEventListener: q,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create replica host: %w", err)
	}
	broker.SetNodeHost(nh)
	broker.SetBrokerShardId(brokerSharID)

	f := func(shardID uint64, replicaID uint64) statemachine.IOnDiskStateMachine {
		return brokerFSM.NewBrokerFSM(shardID, replicaID, broker, mdStorage)
	}
	inviteMembers := raftConfig.InviteMembers
	if raftConfig.Join {
		inviteMembers = nil
	}
	err = nh.StartOnDiskReplica(inviteMembers, raftConfig.Join, f, drConfig.Config{
		ReplicaID:           raftConfig.ReplicaID,
		ShardID:             broker.BrokerShardId(),
		ElectionRTT:         10,
		HeartbeatRTT:        1,
		CheckQuorum:         true,
		OrderedConfigChange: true,
		SnapshotEntries:     raftConfig.Metadata.SnapshotEntries,
		CompactionOverhead:  raftConfig.Metadata.CompactionOverhead,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start replica: %w", err)
	}
	if _, err := q.blockTillLeaderSet(pCtx, broker.BrokerShardId()); err != nil {
		return nil, fmt.Errorf("block till leader set: %w", err)
	}
	if err := q.registerBroker(pCtx); err != nil {
		return nil, fmt.Errorf("failed to register broker: %w", err)
	}
	if err := q.reShardExistingPartitions(pCtx); err != nil {
		return nil, fmt.Errorf("failed to re shard existing partitions: %w", err)
	}
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
	encoderDecoder, err := factory.BrokerEncoderDecoder(pbCommandTypes.Kind_KIND_CREATE_TOPIC)
	if err != nil {
		return nil, fmt.Errorf("get encoder decoder for create topic: %w", err)
	}
	cmdBytes, err := encoderDecoder.EncodeArgs(pCtx, pbBrokerCommands.CreateTopicInputs{
		Topic:           name,
		NumOfPartitions: numberOfPartitions,
		ShardOffset:     q.broker.BrokerShardId() + 1,
	})
	if err != nil {
		return nil, fmt.Errorf("encode args for create topic: %w", err)
	}
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return nil, fmt.Errorf("propose create topic: %w", err)
	}
	results, err := encoderDecoder.DecodeResults(pCtx, res.Data)
	if err != nil {
		return nil, fmt.Errorf("decode results for create topic: %w", err)
	}
	createTopicResult, ok := results.(*pbBrokerCommands.CreateTopicOutputs)
	if !ok {
		return nil, fmt.Errorf("unexpected result type for create topic: %T", results)
	}
	if !createTopicResult.IsCreated {
		return nil, errors.ErrTopicAlreadyExists
	}
	topic := createTopicResult.Topic
	// get all the partitions of the topic, create a shard per partition, randomly add 3 nodes per partition
	cmd := command.Cmd{
		CommandType: command.PartitionsCommands.PartitionsForTopic,
		Args:        [][]byte{[]byte(topic.Topic)},
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
		brokers := util.Sample(util.Keys(membership.Nodes), int(replicationFactor))
		brokerTargets := make(map[uint64]string)
		for _, broker := range brokers {
			brokerTargets[broker] = membership.Nodes[broker]
		}
		members, err := json.Marshal(brokerTargets)
		if err != nil {
			return nil, fmt.Errorf("marshal members: %w", err)
		}
		cmd = command.Cmd{
			CommandType: command.PartitionsCommands.PartitionAdded,
			Args: [][]byte{
				[]byte(partition.ID),
				[]byte(strconv.FormatUint(partition.ShardID, 10)),
				members,
			},
		}
		cmdBytes, err = json.Marshal(cmd)
		if err != nil {
			return nil, fmt.Errorf("marshal cmd: %w", err)
		}
		ctx, cancelFunc = context.WithTimeout(pCtx, 15*time.Second)
		res, err = nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
		cancelFunc()
		if err != nil {
			return nil, fmt.Errorf("propose partition added: %w", err)
		}
		if _, ok := brokerTargets[q.broker.ID]; ok {
			// block for leader set only if current broker is member of the shard
			if _, err := q.blockTillLeaderSet(pCtx, partition.ShardID); err != nil {
				return nil, err
			}
		}
	}

	return &model.Topic{
		Name:               topic.Topic,
		NumberOfPartitions: topic.NumOfPartitions,
	}, nil
}

func (q *Queue) disconnectInActiveConsumers(pCtx context.Context) {
	ticker := time.NewTicker(config.Conf.ConsumerHealthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pCtx.Done():
			return
		case <-ticker.C:
			cmd := command.Cmd{
				CommandType: command.ConsumerCommands.Consumers,
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
			select {
			case <-pCtx.Done():
				return
			default:
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
}

func (q *Queue) SendMessage(
	pCtx context.Context,
	msg *model.Message,
) (*model.Message, error) {
	nh := q.broker.NodeHost()
	partitionID, err := q.topicService.PartitionID(pCtx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitionID: %w", err)
	}
	partition, err := q.topicService.GetPartition(pCtx, partitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition: %w", err)
	}
	msg.PartitionID = partitionID
	msgBytes, err := json.Marshal(msg)
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
	cmd := command.Cmd{
		CommandType: command.MessageCommands.Append,
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
	topics = util.Keys(util.ToSet(topics))
	topicsBytes, err := json.Marshal(topics)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal topics: %w", err)
	}
	cmd := command.Cmd{
		CommandType: command.ConsumerCommands.Connect,
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
		Consumer      *model.Consumer      `json:"Consumer,omitempty"`
		Group         *model.ConsumerGroup `json:"Group,omitempty"`
		TopicNotFound bool                 `json:"TopicNotFound"`
	}
	if err := json.Unmarshal(res.Data, &result); err != nil {
		return nil, nil, fmt.Errorf("un marshall result: %w", err)
	}
	if result.TopicNotFound {
		return nil, nil, errors.ErrTopicNotFound
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

	cmd := command.Cmd{
		CommandType: command.ConsumerCommands.Disconnected,
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
	cmd := command.Cmd{
		CommandType: command.ConsumerCommands.ConsumerForID,
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
		cmd = command.Cmd{
			CommandType: command.MessageCommands.Poll,
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
	cmd = command.Cmd{
		CommandType: command.ConsumerCommands.UpdateConsumer,
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

func (q *Queue) ReceiveMessageForPartition(
	pCtx context.Context,
	consumerID string,
	partitionId string,
) (*model.Message, error) {
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	cmd := command.Cmd{
		CommandType: command.ConsumerCommands.ConsumerForID,
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
	_, ok := util.FirstMatch(consumer.Partitions, func(p string) bool {
		return p == partitionId
	})
	if !ok {
		return nil, fmt.Errorf("consumer %s is not subscribed to partition %s", consumerID, partitionId)
	}

	var msg model.Message
	shardID, ok := q.broker.ShardForPartition(partitionId)
	if !ok {
		return nil, fmt.Errorf("broker does not have partition: %s", partitionId)
	}
	log.Println("consumer polling shardID", shardID, " for partition ID ", partitionId)
	cmd = command.Cmd{
		CommandType: command.MessageCommands.Poll,
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
		return nil, nil
	}
	if err := json.Unmarshal(result.([]byte), &msg); err != nil {
		return nil, fmt.Errorf("un marshall result: %w", err)
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
	cmd := command.Cmd{
		CommandType: command.ConsumerCommands.ConsumerForID,
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
	cmd = command.Cmd{
		CommandType: command.MessageCommands.Ack,
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
	cmd := command.Cmd{
		CommandType: command.ConsumerCommands.HealthCheck,
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

func (q *Queue) ShardsInfo(
	pCtx context.Context,
	topics []string,
) (map[string]*model.ShardInfo, []*model.Broker, *model.Broker, error) {
	cmd := command.Cmd{
		CommandType: command.PartitionsCommands.AllPartitions,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal cmd: %w", err)
	}
	nh := q.broker.NodeHost()
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	res, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
	cancelFunc()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("propose get consumer for ID: %w", err)
	}
	partitions := make([]*model.Partition, 0)
	if err := json.Unmarshal(res.([]byte), &partitions); err != nil {
		return nil, nil, nil, fmt.Errorf("un marshall result: %w", err)
	}
	if len(topics) != 0 {
		topicsSet := util.ToSet(topics)
		partitions = util.Filter(partitions, func(p *model.Partition) bool {
			return topicsSet[p.TopicName]
		})
		if len(partitions) == 0 {
			return nil, nil, nil, fmt.Errorf("no partitions found for topics: %v", topics)
		}
	}
	partitionsBytes, err := json.Marshal(partitions)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal partitions: %w", err)
	}
	var (
		wg             sync.WaitGroup
		shardInfoErr   error
		clusterDetails struct {
			ShardInfo map[string]*model.ShardInfo `json:"shardInfo"`
			Brokers   []*model.Broker             `json:"brokers"`
		}
		leaderBrokerID  uint64
		leaderBroker    *model.Broker
		leaderBrokerErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cmd = command.Cmd{
			CommandType: command.BrokerCommands.ShardInfoForPartitions,
			Args: [][]byte{
				partitionsBytes,
			},
		}
		cmdBytes, err = json.Marshal(cmd)
		if err != nil {
			shardInfoErr = fmt.Errorf("marshal cmd: %w", err)
			return
		}
		ctx, cancelFunc = context.WithTimeout(pCtx, 15*time.Second)
		defer cancelFunc()
		res, err = nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
		if err != nil {
			shardInfoErr = fmt.Errorf("propose get consumer for ID: %w", err)
			return
		}
		if err := json.Unmarshal(res.([]byte), &clusterDetails); err != nil {
			shardInfoErr = fmt.Errorf("un marshall result: %w", err)
			return
		}
		return
	}()

	go func() {
		defer wg.Done()
		leaderID, err := q.blockTillLeaderSet(pCtx, q.broker.BrokerShardId())
		if err != nil {
			leaderBrokerErr = err
			return
		}
		leaderBrokerID = leaderID
	}()
	wg.Wait()
	if shardInfoErr != nil || leaderBrokerErr != nil {
		return nil, nil, nil, fmt.Errorf(
			"failed to get shard info or leader broker: %w, %w",
			shardInfoErr,
			leaderBrokerErr,
		)
	}
	for _, broker := range clusterDetails.Brokers {
		if broker.ID == leaderBrokerID {
			leaderBroker = broker
		}
	}
	if leaderBrokerID != 0 && leaderBroker == nil {
		return nil, nil, nil, fmt.Errorf("leader broker not found for ID: %d", leaderBrokerID)
	}
	return clusterDetails.ShardInfo, clusterDetails.Brokers, leaderBroker, nil
}

func (q *Queue) RegisterNewNode(
	pCtx context.Context,
	newNodeID uint64,
	targetNodeAddr string,
) error {
	nh := q.broker.NodeHost()
	leaderID, _, _, err := nh.GetLeaderID(q.broker.BrokerShardId())
	if err != nil {
		return fmt.Errorf("failed to get leader ID: %w", err)
	}
	if leaderID != q.broker.ID {
		return errors.ErrCurrentNodeNotLeader
	}
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	membership, err := nh.SyncGetShardMembership(ctx, q.broker.BrokerShardId())
	if err != nil {
		return fmt.Errorf("failed to get shard membership: %w", err)
	}
	if _, ok := membership.Nodes[newNodeID]; ok {
		return errors.ErrBrokerAlreadyExists
	}
	if err := nh.SyncRequestAddReplica(
		ctx,
		q.broker.BrokerShardId(),
		newNodeID,
		targetNodeAddr,
		membership.ConfigChangeID,
	); err != nil {
		return fmt.Errorf("failed to add new replica: %w", err)
	}
	_, err = q.blockTillLeaderSet(pCtx, q.broker.BrokerShardId())
	return err
}

func (q *Queue) blockTillLeaderSet(
	pCtx context.Context,
	shardID uint64,
) (uint64, error) {
	leaderID, _, ok, err := q.broker.NodeHost().GetLeaderID(shardID)
	if err != nil {
		return 0, fmt.Errorf("leader for broker shard: %w", err)
	}
	if ok {
		return leaderID, nil
	}
	ctx, cancelFunc := context.WithTimeout(pCtx, config.Conf.ShardLeaderWaitTime)
	ticker := time.NewTicker(config.Conf.ShardLeaderSetReCheckInterval)
	log.Println("finding leader for shardID: ", shardID)
	defer ticker.Stop()
	defer cancelFunc()
	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("timeout waiting for leader to be set")
		case <-ticker.C:
			leaderID, _, ok, err := q.broker.NodeHost().GetLeaderID(shardID)
			if err != nil {
				return 0, fmt.Errorf("leader for broker shard: %w", err)
			}
			if ok {
				return leaderID, nil
			}
		}
	}
}

func (q *Queue) reShardExistingPartitions(pCtx context.Context) error {
	nh := q.broker.NodeHost()
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	cmd := command.Cmd{
		CommandType: command.PartitionsCommands.AllPartitions,
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
				return messageFSM.NewMessageFSM(shardID, replicdID, q.broker, q.mdStorage)
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
		if _, err := q.blockTillLeaderSet(pCtx, shardID); err != nil {
			return fmt.Errorf("block till leader set: %w, for shardID: %d", err, shardID)
		}
	}

	return nil
}

func (q *Queue) registerBroker(pCtx context.Context) error {
	nh := q.broker.NodeHost()
	brokerBytes, err := json.Marshal(q.broker)
	if err != nil {
		return fmt.Errorf("marshal broker: %w", err)
	}
	cmd := command.Cmd{
		CommandType: command.BrokerCommands.RegisterBroker,
		Args: [][]byte{
			brokerBytes,
		},
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal cmd: %w", err)
	}
	ctx, cancelFunc := context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	_, err = nh.SyncPropose(ctx, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		return fmt.Errorf("propose register broker: %w", err)
	}
	return nil
}

func (q *Queue) LeaderUpdated(info raftio.LeaderInfo) {
	if info.ShardID != q.broker.BrokerShardId() {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.prevBrokerShardLeaderID == q.broker.ID && info.LeaderID != q.broker.ID {
		if q.deactivateConsumerCancel != nil {
			q.deactivateConsumerCancel()
			q.deactivateConsumerCancel = nil
		}
	} else if q.prevBrokerShardLeaderID != q.broker.ID && info.LeaderID == q.broker.ID {
		ctx, cancel := context.WithCancel(q.pCtx)
		q.deactivateConsumerCancel = cancel
		go q.disconnectInActiveConsumers(ctx)
	}
	q.prevBrokerShardLeaderID = info.LeaderID
}
