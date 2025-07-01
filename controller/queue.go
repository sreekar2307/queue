package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"

	"github.com/sreekar2307/queue/logger"
	"go.opentelemetry.io/otel/trace"

	pbMessageCommand "github.com/sreekar2307/queue/gen/raft/fsm/message/v1"

	"github.com/sreekar2307/queue/raft/fsm/command/factory"
	fsmFactory "github.com/sreekar2307/queue/raft/fsm/factory"
	"github.com/sreekar2307/queue/service"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	dragonLogger "github.com/lni/dragonboat/v4/logger"
	pbBrokerCommands "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
)

const (
	brokerSharID = 101
)

type Queue struct {
	broker *model.Broker
	tracer trace.Tracer

	mdStorage      storage.MetadataStorage
	topicService   service.TopicService
	messageService service.MessageService
	pCtx           context.Context

	mu                       sync.Mutex
	deactivateConsumerCancel context.CancelFunc
	prevBrokerShardLeaderID  uint64
	log                      logger.Logger
}

func NewQueue(
	pCtx context.Context,
	tracer trace.Tracer,
	log logger.Logger,
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
	mdStorage := metadataStorage.NewBolt(filepath.Join(metadataPath, "metadata.db"), tracer)
	if err := mdStorage.Open(pCtx); err != nil {
		return nil, fmt.Errorf("failed to open metadata storage: %w", err)
	}
	broker := &model.Broker{
		ID:               raftConfig.ReplicaID,
		RaftAddress:      raftConfig.Addr,
		ReachGrpcAddress: conf.GRPC.ListenerAddr,
		ReachHttpAddress: conf.HTTP.ListenerAddr,
		StartMsgFSM:      make(chan *model.Partition),
	}
	q := &Queue{
		broker:       broker,
		mdStorage:    mdStorage,
		topicService: topicServ.NewTopicService(mdStorage, log),
		pCtx:         pCtx,
		log:          log,
		tracer:       tracer,
	}
	dragonLogger.SetLoggerFactory(func(pgk string) dragonLogger.ILogger {
		return log.WithFields(logger.NewAttr("package", pgk))
	})
	go q.listenForJoinShard(pCtx)
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
		return fsmFactory.NewBrokerFSM(shardID, replicaID, broker, tracer, log, mdStorage)
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
) (*pbTypes.Topic, error) {
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_CREATE_TOPIC)
	if err != nil {
		return nil, fmt.Errorf("get encoder decoder for create topic: %w", err)
	}
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "createTopic"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("createTopic"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	cmdBytes, err := encoderDecoder.EncodeArgs(ctx, &pbBrokerCommands.CreateTopicInputs{
		Topic:           name,
		NumOfPartitions: numberOfPartitions,
		ShardOffset:     q.broker.BrokerShardId() + 1,
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to propose create topic")
		span.End()
		return nil, fmt.Errorf("encode args for create topic: %w", err)
	}
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	res, err := q.broker.NodeHost().SyncPropose(ctxTimeout, q.broker.NodeHost().GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to propose create topic")
		span.End()
		return nil, fmt.Errorf("propose create topic: %w", err)
	}
	results, err := encoderDecoder.DecodeResults(pCtx, res.Data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to propose create topic")
		span.End()
		return nil, fmt.Errorf("decode results for create topic: %w", err)
	}
	createTopicResult, ok := results.(*pbBrokerCommands.CreateTopicOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for create topic: %T", results)
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to propose create topic")
		span.End()
		return nil, err
	}
	if !createTopicResult.IsCreated {
		span.RecordError(errors.ErrTopicAlreadyExists)
		span.SetStatus(codes.Error, "failed to propose create topic")
		span.End()
		return nil, errors.ErrTopicAlreadyExists
	}
	topic := createTopicResult.Topic
	// get all the partitions of the topic, create a shard per partition, randomly add 3 nodes per partition
	encoderDecoder, err = factory.EncoderDecoder(pbCommandTypes.Kind_KIND_PARTITIONS_FOR_TOPIC)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to propose create topic")
		span.End()
		return nil, fmt.Errorf("get encoder decoder for get partitions: %w", err)
	}
	span.End()
	ctx, span = q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "partitionsForTopic"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("partitionsForTopic"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	headers = make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	cmdBytes, err = encoderDecoder.EncodeArgs(pCtx, &pbBrokerCommands.PartitionsForTopicInputs{
		Topic: topic.Topic,
	}, headers)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctxTimeout, cancelFunc = context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	numPartitionsRes, err := q.broker.NodeHost().SyncRead(ctxTimeout, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to read get partitions")
		span.End()
		return nil, fmt.Errorf("sync read get partitions: %w", err)
	}
	results, err = encoderDecoder.DecodeResults(pCtx, numPartitionsRes.([]byte))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to read get partitions")
		span.End()
		return nil, fmt.Errorf("decode results for get partitions: %w", err)
	}
	partitionsForTopicOutputs, ok := results.(*pbBrokerCommands.PartitionsForTopicOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for get partitions: %T", results)
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to read get partitions")
		span.End()
		return nil, err
	}
	span.End()
	ctx, span = q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "syncGetShardMembership"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("syncGetShardMembership"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	headers = make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	ctxTimeout, cancelFunc = context.WithTimeout(pCtx, 15*time.Second)
	defer cancelFunc()
	membership, err := q.broker.NodeHost().SyncGetShardMembership(ctxTimeout, q.broker.BrokerShardId())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to read get shard membership")
		span.End()
		return nil, fmt.Errorf("get shard membership: %w", err)
	}
	span.End()
	for _, partition := range partitionsForTopicOutputs.Partitions {
		brokers := util.Sample(util.Keys(membership.Nodes), int(replicationFactor))
		brokerTargets := make(map[uint64]string)
		for _, broker := range brokers {
			brokerTargets[broker] = membership.Nodes[broker]
		}
		encoderDecoder, err = factory.EncoderDecoder(pbCommandTypes.Kind_KIND_PARTITION_ADDED)
		if err != nil {
			return nil, fmt.Errorf("get encoder decoder for partition added: %w", err)
		}

		ctx, span = q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "partitionAdded"),
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				semconv.RPCSystemKey.String("raft"),
				semconv.ServerAddressKey.String(q.broker.RaftAddress),
				semconv.RPCMethodKey.String("partitionAdded"),
				semconv.RPCServiceKey.String("raft.broker.FSM"),
			),
		)
		headers = make(propagation.MapCarrier)
		otel.GetTextMapPropagator().Inject(ctx, &headers)
		cmdBytes, err = encoderDecoder.EncodeArgs(pCtx, &pbBrokerCommands.PartitionAdddedInputs{
			PartitionId: partition.Id,
			ShardId:     partition.ShardId,
			Members:     brokerTargets,
		}, headers)
		if err != nil {
			return nil, fmt.Errorf("marshal cmd: %w", err)
		}
		ctxTimeout, cancelFunc = context.WithTimeout(ctx, 15*time.Second)
		_, err = q.broker.NodeHost().SyncPropose(ctxTimeout, q.broker.NodeHost().GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
		cancelFunc()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to propose partition added")
			span.End()
			return nil, fmt.Errorf("propose partition added: %w", err)
		}
		span.End()
	}

	return topic, nil
}

func (q *Queue) disconnectInActiveConsumers(pCtx context.Context) {
	ticker := time.NewTicker(config.Conf.ConsumerHealthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pCtx.Done():
			return
		case <-ticker.C:
			encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_CONSUMERS)
			if err != nil {
				q.log.Error(pCtx, "failed to get encoder decoder for consumers: ", logger.NewAttr("error", err))
				return
			}
			ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "allConsumers"),
				trace.WithSpanKind(trace.SpanKindClient),
				trace.WithAttributes(
					semconv.RPCSystemKey.String("raft"),
					semconv.ServerAddressKey.String(q.broker.RaftAddress),
					semconv.RPCMethodKey.String("allConsumers"),
					semconv.RPCServiceKey.String("raft.broker.FSM"),
				),
			)
			headers := make(propagation.MapCarrier)
			otel.GetTextMapPropagator().Inject(ctx, &headers)
			nh := q.broker.NodeHost()
			cmdBytes, err := encoderDecoder.EncodeArgs(pCtx, nil, headers)
			if err != nil {
				q.log.Error(pCtx, "failed to marshal cmd", logger.NewAttr("error", err))
				return
			}
			ctx, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
			res, err := nh.SyncRead(ctx, q.broker.BrokerShardId(), cmdBytes)
			cancelFunc()
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to propose health check")
				span.End()
				q.log.Error(pCtx, "failed to propose health check: ", logger.NewAttr("error", err))
				return
			}
			span.End()
			select {
			case <-pCtx.Done():
				return
			default:
			}
			results, err := encoderDecoder.DecodeResults(pCtx, res.([]byte))
			if err != nil {
				q.log.Error(pCtx, "failed to decode results for consumers: ", logger.NewAttr("error", err))
				return
			}
			consumersResults, ok := results.(*pbBrokerCommands.ConsumersOutputs)
			if !ok {
				q.log.Errorf("unexpected result type for consumers: %T", results)
				return
			}
			for _, consumerPb := range consumersResults.Consumers {
				if consumerPb.LastHealthCheckAt.Seconds < time.Now().Add(-config.Conf.ConsumerLostTime).Unix() {
					if err := q.disconnect(pCtx, consumerPb.Id); err != nil {
						q.log.Error(pCtx, "failed to disconnect consumer",
							logger.NewAttr("consumerID", consumerPb.Id), logger.NewAttr("error", err))
					} else {
						q.log.Warn(pCtx, "disconnected consumer", logger.NewAttr("consumerID", consumerPb.Id))
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
	shardID, ok := q.broker.ShardForPartition(partitionID)
	if !ok {
		return nil, fmt.Errorf("failed to get shardID for partition: %s", partitionID)
	}
	if partition.ShardID != shardID {
		return nil, fmt.Errorf("shardID mismatch: %d != %d", partition.ShardID, shardID)
	}
	q.log.Info(pCtx, "sending message to", logger.NewAttr("selectedShard", shardID),
		logger.NewAttr("partition", partitionID))
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_MESSAGE_APPEND)
	if err != nil {
		return nil, fmt.Errorf("get encoder decoder for append message: %w", err)
	}
	ci := &pbMessageCommand.AppendInputs{
		Message: msg.ToProtoBuf(),
	}
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/message/FSM", "appendMessage"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("appendMessage"),
			semconv.RPCServiceKey.String("raft.message.FSM"),
		),
	)
	defer span.End()
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	cmdBytes, err := encoderDecoder.EncodeArgs(ctx, ci, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	res, err := nh.SyncPropose(ctxTimeout, nh.GetNoOPSession(partition.ShardID), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "propose append messages")
		return nil, fmt.Errorf("propose append messages: %w", err)
	}
	results, err := encoderDecoder.DecodeResults(ctxTimeout, res.Data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode results for append messages")
		return nil, fmt.Errorf("decode results for append messages: %w", err)
	}
	appendMessageOutputs, ok := results.(*pbMessageCommand.AppendOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for append messages: %T", results)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unexpected result type")
		return nil, err
	}
	return model.FromProtoBufMessage(appendMessageOutputs.Message), nil
}

func (q *Queue) Connect(
	pCtx context.Context,
	consumerID, consumerGroupID string,
	topics []string,
) (*pbTypes.Consumer, *pbTypes.ConsumerGroup, error) {
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_CONNECT)
	if err != nil {
		return nil, nil, fmt.Errorf("get encoder decoder for connect: %w", err)
	}
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "connect"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("connect"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	defer span.End()
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	cmdBytes, err := encoderDecoder.EncodeArgs(ctx, &pbBrokerCommands.ConnectInputs{
		ConsumerGroupId: consumerGroupID,
		ConsumerId:      consumerID,
		Topics:          topics,
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "encode args for connect")
		return nil, nil, fmt.Errorf("encode args for connect: %w", err)
	}
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	res, err := nh.SyncPropose(ctxTimeout, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "propose connect")
		return nil, nil, fmt.Errorf("propose connect: %w", err)
	}
	results, err := encoderDecoder.DecodeResults(ctxTimeout, res.Data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode results for connect")
		return nil, nil, fmt.Errorf("decode results for connect: %w", err)
	}
	connectResults, ok := results.(*pbBrokerCommands.ConnectOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for connect: %T", results)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unexpected result type")
		return nil, nil, err
	}
	if connectResults.TopicsNotFound {
		span.RecordError(errors.ErrTopicNotFound)
		span.SetStatus(codes.Error, "topics not found")
		return nil, nil, errors.ErrTopicNotFound
	}
	return connectResults.Consumer, connectResults.ConsumerGroup, nil
}

func (q *Queue) disconnect(
	pCtx context.Context,
	consumerID string,
) error {
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_DISCONNECT)
	if err != nil {
		return fmt.Errorf("get encoder decoder for disconnect: %w", err)
	}
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "disconnect"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("disconnect"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	defer span.End()
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	cmdBytes, err := encoderDecoder.EncodeArgs(ctx, &pbBrokerCommands.DisconnectInputs{
		ConsumerId: consumerID,
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "encode args for disconnect")
		return fmt.Errorf("encode args for disconnect: %w", err)
	}
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	_, err = nh.SyncPropose(ctxTimeout, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "propose disconnect")
		return fmt.Errorf("propose disconnect: %w", err)
	}
	return nil
}

func (q *Queue) ReceiveMessageForPartition(
	pCtx context.Context,
	consumerID string,
	partitionId string,
) (*model.Message, error) {
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "consumerForID"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("consumerForID"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_CONSUMER_FOR_ID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "get encoder decoder for consumer for ID")
		span.End()
		return nil, fmt.Errorf("get encoder decoder for consumer for ID: %w", err)
	}
	cmdBytes, err := encoderDecoder.EncodeArgs(pCtx, &pbBrokerCommands.ConsumerForIDInputs{
		ConsumerId: consumerID,
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		span.End()
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	res, err := nh.SyncRead(ctxTimeout, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "propose get consumer for ID")
		span.End()
		return nil, fmt.Errorf("propose get consumer for ID: %w", err)
	}
	results, err := encoderDecoder.DecodeResults(pCtx, res.([]byte))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode results for get consumer for ID")
		span.End()
		return nil, fmt.Errorf("decode results for get consumer for ID: %w", err)
	}
	output, ok := results.(*pbBrokerCommands.ConsumerForIDOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for get consumer for ID: %T", results)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unexpected result type")
		span.End()
		return nil, err
	}
	span.End()
	consumer := model.FromProtoBufConsumer(output.Consumer)
	_, ok = util.FirstMatch(consumer.Partitions, func(p string) bool {
		return p == partitionId
	})
	if !ok {
		return nil, fmt.Errorf("consumer %s is not subscribed to partition %s", consumerID, partitionId)
	}
	shardID, ok := q.broker.ShardForPartition(partitionId)
	if !ok {
		return nil, fmt.Errorf("broker does not have partition: %s", partitionId)
	}
	q.log.Info(
		pCtx,
		"consumer polling",
		logger.NewAttr("shardID", shardID),
		logger.NewAttr("partitionId", partitionId),
	)
	ctx, span = q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/message/FSM", "pollMessage"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("pollMessage"),
			semconv.RPCServiceKey.String("raft.message.FSM"),
		),
	)
	headers = make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	encoderDecoder, err = factory.EncoderDecoder(pbCommandTypes.Kind_KIND_MESSAGE_POLL)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "get encoder decoder for poll message")
		span.End()
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	cmdBytes, err = encoderDecoder.EncodeArgs(pCtx, &pbMessageCommand.PollInputs{
		ConsumerGroupId: consumer.ConsumerGroup,
		PartitionId:     partitionId,
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		span.End()
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	result, err := nh.SyncRead(ctxTimeout, shardID, cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "sync read get message")
		span.End()
		return nil, fmt.Errorf("sync read get message: %w", err)
	}
	outputs, err := encoderDecoder.DecodeResults(ctx, result.([]byte))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode results for get message")
		span.End()
		return nil, fmt.Errorf("decode results for get message: %w", err)
	}
	msgOutputs, ok := outputs.(*pbMessageCommand.PollOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for get message: %T", outputs)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unexpected result type")
		span.End()
		return nil, err
	}
	span.End()
	if msgOutputs.Message == nil {
		return nil, nil
	}
	return model.FromProtoBufMessage(msgOutputs.Message), nil
}

func (q *Queue) AckMessage(
	pCtx context.Context,
	consumerID string,
	msg *model.Message,
) error {
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "consumerForID"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("consumerForID"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_CONSUMER_FOR_ID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "get encoder decoder for consumer for ID")
		span.End()
		return fmt.Errorf("get encoder decoder for consumer for ID: %w", err)
	}
	cmdBytes, err := encoderDecoder.EncodeArgs(pCtx, &pbBrokerCommands.ConsumerForIDInputs{
		ConsumerId: consumerID,
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		span.End()
		return fmt.Errorf("marshal cmd: %w", err)
	}
	res, err := nh.SyncRead(ctxTimeout, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "propose get consumer for ID")
		span.End()
		return fmt.Errorf("propose get consumer for ID: %w", err)
	}
	results, err := encoderDecoder.DecodeResults(pCtx, res.([]byte))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode results for get consumer for ID")
		span.End()
		return fmt.Errorf("decode results for get consumer for ID: %w", err)
	}
	output, ok := results.(*pbBrokerCommands.ConsumerForIDOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for get consumer for ID: %T", results)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unexpected result type")
		span.End()
		return err
	}
	span.End()
	consumer := model.FromProtoBufConsumer(output.Consumer)
	shardID, ok := q.broker.ShardForPartition(msg.PartitionID)
	if !ok {
		return fmt.Errorf("broker does not have partition: %s", msg.PartitionID)
	}
	ctx, span = q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/message/FSM", "ackMessage"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("ackMessage"),
			semconv.RPCServiceKey.String("raft.message.FSM"),
		),
	)
	headers = make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	encoderDecoder, err = factory.EncoderDecoder(pbCommandTypes.Kind_KIND_MESSAGE_ACK)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "get encoder decoder for ack message")
		span.End()
		return fmt.Errorf("get encoder decoder for ack message: %w", err)
	}
	cmdBytes, err = encoderDecoder.EncodeArgs(ctx, &pbMessageCommand.AckInputs{
		ConsumerGroupId: consumer.ConsumerGroup,
		Message:         msg.ToProtoBuf(),
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		span.End()
		return fmt.Errorf("marshal cmd: %w", err)
	}
	headers = make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	cmdBytes, err = encoderDecoder.EncodeArgs(ctx, &pbMessageCommand.AckInputs{
		ConsumerGroupId: consumer.ConsumerGroup,
		Message:         msg.ToProtoBuf(),
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		span.End()
		return fmt.Errorf("marshal cmd: %w", err)
	}
	ctxTimeout, cancelFunc = context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	_, err = nh.SyncPropose(ctxTimeout, nh.GetNoOPSession(shardID), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "sync read get message")
		span.End()
		return fmt.Errorf("sync read get message: %w", err)
	}
	span.End()
	return nil
}

func (q *Queue) HealthCheck(
	pCtx context.Context,
	consumerID string,
	healthCheckAt time.Time,
) (*model.Consumer, error) {
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_HEALTH_CHECK)
	if err != nil {
		return nil, fmt.Errorf("get encoder decoder for health check: %w", err)
	}
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "healthCheck"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("healthCheck"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	defer span.End()
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	cmdBytes, err := encoderDecoder.EncodeArgs(ctx, &pbBrokerCommands.HealthCheckInputs{
		ConsumerId: consumerID,
		PingAt:     timestamppb.New(healthCheckAt),
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	res, err := q.broker.NodeHost().SyncPropose(ctxTimeout, q.broker.NodeHost().GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "propose health check")
		return nil, fmt.Errorf("propose health check: %w", err)
	}
	output, err := encoderDecoder.DecodeResults(ctxTimeout, res.Data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode results for health check")
		return nil, fmt.Errorf("decode results for health check: %w", err)
	}
	healthCheckOutputs, ok := output.(*pbBrokerCommands.HealthCheckOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for health check: %T", output)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unexpected result type")
		return nil, err
	}
	return model.FromProtoBufConsumer(healthCheckOutputs.Consumer), nil
}

func (q *Queue) ShardsInfo(
	pCtx context.Context,
	topics []string,
) (map[string]*model.ShardInfo, []*model.Broker, *model.Broker, error) {
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "allPartitions"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("allPartitions"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_ALL_PARTITIONS)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "get encoder decoder for all partitions")
		span.End()
		return nil, nil, nil, fmt.Errorf("get encoder decoder for all partitions: %w", err)
	}
	cmdBytes, err := encoderDecoder.EncodeArgs(ctx, nil, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		span.End()
		return nil, nil, nil, fmt.Errorf("marshal cmd: %w", err)
	}
	nh := q.broker.NodeHost()
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	res, err := nh.SyncRead(ctxTimeout, q.broker.BrokerShardId(), cmdBytes)
	cancelFunc()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "propose get consumer for ID")
		span.End()
		return nil, nil, nil, fmt.Errorf("propose get consumer for ID: %w", err)
	}
	results, err := encoderDecoder.DecodeResults(ctx, res.([]byte))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode results for all partitions")
		span.End()
		return nil, nil, nil, fmt.Errorf("decode results for all partitions: %w", err)
	}
	allPartitionsOutputs, ok := results.(*pbBrokerCommands.AllPartitionsOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for all partitions: %T", results)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unexpected result type")
		span.End()
		return nil, nil, nil, err
	}
	span.End()
	partitions := allPartitionsOutputs.Partitions
	if len(topics) != 0 {
		topicsSet := util.ToSet(topics)
		partitions = util.Filter(allPartitionsOutputs.Partitions, func(p *pbTypes.Partition) bool {
			return topicsSet[p.Topic]
		})
		if len(partitions) == 0 {
			return nil, nil, nil, fmt.Errorf("no partitions found for topics: %v", topics)
		}
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
		ctx, span = q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "shardInfoForPartitions"),
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				semconv.RPCSystemKey.String("raft"),
				semconv.ServerAddressKey.String(q.broker.RaftAddress),
				semconv.RPCMethodKey.String("shardInfoForPartitions"),
				semconv.RPCServiceKey.String("raft.broker.FSM"),
			),
		)
		defer span.End()
		headers = make(propagation.MapCarrier)
		otel.GetTextMapPropagator().Inject(ctx, &headers)
		encoderDecoder, err = factory.EncoderDecoder(pbCommandTypes.Kind_KIND_SHARD_INFO_FOR_PARTITIONS)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "get encoder decoder for shard info")
			shardInfoErr = fmt.Errorf("get encoder decoder for shard info: %w", err)
			return
		}
		cmdBytes, err = encoderDecoder.EncodeArgs(ctx, &pbBrokerCommands.ShardInfoForPartitionsInputs{
			Partitions: partitions,
		}, headers)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "marshal cmd")
			shardInfoErr = fmt.Errorf("marshal cmd: %w", err)
			return
		}
		ctxTimeout, cancelFunc = context.WithTimeout(ctx, 15*time.Second)
		defer cancelFunc()
		res, err = nh.SyncRead(ctxTimeout, q.broker.BrokerShardId(), cmdBytes)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "propose get consumer for ID")
			shardInfoErr = fmt.Errorf("propose get consumer for ID: %w", err)
			return
		}
		co, err := encoderDecoder.DecodeResults(ctx, res.([]byte))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "decode results for shard info")
			shardInfoErr = fmt.Errorf("decode results for shard info: %w", err)
			return
		}
		shardInfoOutputs, ok := co.(*pbBrokerCommands.ShardInfoForPartitionsOutputs)
		if !ok {
			err := fmt.Errorf("unexpected result type for shard info: %T", co)
			span.RecordError(err)
			span.SetStatus(codes.Error, "unexpected result type")
			shardInfoErr = err
			return
		}
		clusterDetails.ShardInfo = make(map[string]*model.ShardInfo)
		for _, si := range shardInfoOutputs.ShardInfo {
			clusterDetails.ShardInfo[si.PartitionId] = model.FromProtoBufShardInfo(si)
		}
		for _, broker := range shardInfoOutputs.Brokers {
			clusterDetails.Brokers = append(clusterDetails.Brokers, model.FromProtoBufBroker(broker))
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
	q.log.Info(pCtx, "finding leader", logger.NewAttr("shardID", shardID))
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
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "allPartitions"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("allPartitions"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_ALL_PARTITIONS)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "get encoder decoder for all partitions")
		span.End()
		return fmt.Errorf("get encoder decoder for all partitions: %w", err)
	}
	cmdBytes, err := encoderDecoder.EncodeArgs(ctx, nil, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal cmd")
		span.End()
		return fmt.Errorf("marshal cmd: %w", err)
	}
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	res, err := nh.SyncRead(ctxTimeout, q.broker.BrokerShardId(), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "sync read all partitions")
		span.End()
		return fmt.Errorf("sync read all partitions: %w", err)
	}
	results, err := encoderDecoder.DecodeResults(pCtx, res.([]byte))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode results for all partitions")
		span.End()
		return fmt.Errorf("decode results for all partitions: %w", err)
	}
	allPartitionsOutputs, ok := results.(*pbBrokerCommands.AllPartitionsOutputs)
	if !ok {
		err := fmt.Errorf("unexpected result type for all partitions: %T", results)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unexpected result type")
		span.End()
		return err
	}
	span.End()
	for _, partition := range allPartitionsOutputs.Partitions {
		if partition.ShardId == 0 || len(partition.Members) == 0 {
			continue
		}
		shardID := partition.ShardId
		if _, ok := partition.Members[q.broker.ID]; !ok {
			continue
		}
		q.broker.JoinShard(model.FromProtoBufPartition(partition))
		q.log.Info(
			pCtx,
			"Starting partition",
			logger.NewAttr("partitionID",
				partition.Id,
			),
			logger.NewAttr("shardID", shardID),
			logger.NewAttr("replicaID", q.broker.ID),
		)
	}

	return nil
}

func (q *Queue) registerBroker(pCtx context.Context) error {
	encoderDecoder, err := factory.EncoderDecoder(pbCommandTypes.Kind_KIND_REGISTER_BROKER)
	if err != nil {
		return fmt.Errorf("get encoder decoder for register broker: %w", err)
	}
	ctx, span := q.tracer.Start(pCtx, fmt.Sprintf("%s/%s/%s", config.Conf.Scope(), "raft/fsm/broker/FSM", "registerBroker"),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.RPCSystemKey.String("raft"),
			semconv.ServerAddressKey.String(q.broker.RaftAddress),
			semconv.RPCMethodKey.String("registerBroker"),
			semconv.RPCServiceKey.String("raft.broker.FSM"),
		),
	)
	defer span.End()
	headers := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	cmdBytes, err := encoderDecoder.EncodeArgs(ctx, &pbBrokerCommands.RegisterBrokerInputs{
		Broker: q.broker.ToProtoBuf(),
	}, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "encode args for register broker")
		return fmt.Errorf("encode args for register broker: %w", err)
	}
	ctxTimeout, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()
	nh := q.broker.NodeHost()
	_, err = nh.SyncPropose(ctxTimeout, nh.GetNoOPSession(q.broker.BrokerShardId()), cmdBytes)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "propose register broker")
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

func (q *Queue) listenForJoinShard(pCtx context.Context) {
	raftConfig := config.Conf.RaftConfig
	for {
		select {
		case <-pCtx.Done():
			return
		case partition := <-q.broker.StartMsgFSM:
			q.log.Info(pCtx, "starting message FSM for partition", logger.NewAttr("partitionID", partition.ID))
			nh := q.broker.NodeHost()
			err := nh.StartOnDiskReplica(
				partition.Members,
				false,
				func(shardID, replicaID uint64) statemachine.IOnDiskStateMachine {
					return fsmFactory.NewMessageFSM(
						shardID,
						replicaID,
						q.broker,
						q.tracer,
						q.log,
						q.mdStorage,
					)
				},
				drConfig.Config{
					ReplicaID:          q.broker.ID,
					ShardID:            partition.ShardID,
					ElectionRTT:        10,
					HeartbeatRTT:       1,
					CheckQuorum:        true,
					SnapshotEntries:    raftConfig.Messages.SnapshotsEntries,
					CompactionOverhead: raftConfig.Messages.CompactionOverhead,
				},
			)
			if err != nil {
				q.log.Error(
					pCtx,
					"ailed to start message FSM for partition",
					logger.NewAttr("partitionID", partition.ID),
					logger.NewAttr("error", err),
				)
				continue
			}
		}
	}
}
