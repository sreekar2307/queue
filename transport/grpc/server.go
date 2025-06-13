package grpc

import (
	"context"
	stdErrors "errors"
	"fmt"
	"github.com/sreekar2307/queue/controller"
	"io"
	"log"
	"net"

	"google.golang.org/grpc/reflection"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc/metadata"

	"buf.build/go/protovalidate"
	provalidateInterceptor "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/sreekar2307/queue/config"
	pb "github.com/sreekar2307/queue/gen/queue/v1"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service/errors"
	"github.com/sreekar2307/queue/util"

	"google.golang.org/grpc"
)

type GRPC struct {
	pb.UnimplementedQueueServiceServer
	queue  *controller.Queue
	server *grpc.Server
	config config.GRPC
}

func NewTransport(
	_ context.Context,
	config config.GRPC,
	queue *controller.Queue,
) (*GRPC, error) {
	g := &GRPC{
		queue:  queue,
		config: config,
	}
	// Create a Protovalidate Validator
	pv, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("create protovalidate validator: %w", err)
	}

	interceptor := provalidateInterceptor.UnaryServerInterceptor(pv)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor),
	)
	server.RegisterService(&pb.QueueService_ServiceDesc, g)
	g.server = server
	reflection.Register(server)
	return g, nil
}

func (g *GRPC) Start(_ context.Context) error {
	lis, err := net.Listen("tcp", g.config.ListenerAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	if err := g.server.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	log.Printf("GRPC Server started at localhost:%d\n", 8000)
	return nil
}

func (g *GRPC) HealthCheck(biStream grpc.BidiStreamingServer[pb.HealthCheckRequest, pb.HealthCheckResponse]) error {
	for {
		ctx := biStream.Context()

		// Receive a health check request from the stream
		req, err := biStream.Recv()
		if err != nil {
			if stdErrors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pingAt := req.GetPingAt().AsTime()

		if _, err := g.queue.HealthCheck(ctx, req.GetConsumerId(), pingAt); err != nil {
			return err
		}

		if err := biStream.Send(&pb.HealthCheckResponse{
			Message: "PONG",
		}); err != nil {
			return err
		}
	}
}

func (g *GRPC) AckMessage(ctx context.Context, req *pb.AckMessageRequest) (*pb.AckMessageResponse, error) {
	if err := g.queue.AckMessage(ctx, req.GetConsumerId(), &model.Message{
		PartitionID: req.GetPartitionId(),
		ID:          req.GetMessageId(),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "ack message: %v", err)
	}
	md := metadata.Pairs(
		PartitionMetadataKey, req.GetPartitionId(),
	)
	_ = grpc.SetTrailer(ctx, md)
	return nil, nil
}

func (g *GRPC) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	msg, err := g.queue.SendMessage(ctx, &model.Message{
		Data:         req.GetData(),
		PartitionKey: req.GetPartitionKey(),
		Topic:        req.GetTopic(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "send message: %v", err)
	}
	md := metadata.Pairs(
		TopicMetadataKey, msg.Topic,
		PartitionMetadataKey, msg.PartitionID,
	)
	_ = grpc.SetTrailer(ctx, md)
	return &pb.SendMessageResponse{
		Topic:        msg.Topic,
		PartitionKey: msg.PartitionKey,
		Data:         msg.Data,
		PartitionId:  msg.PartitionID,
		MessageId:    msg.ID,
	}, nil
}

func (g *GRPC) ReceiveMessageForPartitionID(
	ctx context.Context,
	req *pb.ReceiveMessageForPartitionIDRequest,
) (*pb.ReceiveMessageForPartitionIDResponse, error) {
	msg, err := g.queue.ReceiveMessageForPartition(
		ctx,
		req.GetConsumerId(),
		req.GetPartitionId(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "receive message: %v", err)
	}
	if msg == nil {
		return nil, nil
	}
	md := metadata.Pairs(
		TopicMetadataKey, msg.Topic,
		PartitionMetadataKey, msg.PartitionID,
	)
	_ = grpc.SetTrailer(ctx, md)
	_ = grpc.SetHeader(ctx, md)
	return &pb.ReceiveMessageForPartitionIDResponse{
		Topic:        msg.Topic,
		PartitionKey: msg.PartitionKey,
		Data:         msg.Data,
		PartitionId:  msg.PartitionID,
		MessageId:    msg.ID,
	}, nil
}

func (g *GRPC) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	err := protovalidate.Validate(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "validate request: %v", err)
	}
	topic, err := g.queue.CreateTopic(ctx,
		req.GetTopic(),
		req.GetNumberOfPartitions(),
		req.GetReplicationFactor(),
	)
	if err != nil {
		if stdErrors.Is(err, errors.ErrTopicAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "topic %s already exists", req.GetTopic())
		}
		return nil, status.Errorf(codes.Internal, "create topic: %v", err)
	}
	md := metadata.Pairs(TopicMetadataKey, topic.Name)
	_ = grpc.SetTrailer(ctx, md)
	return &pb.CreateTopicResponse{
		Name:               topic.Name,
		NumberOfPartitions: topic.NumberOfPartitions,
	}, nil
}

func (g *GRPC) Connect(ctx context.Context, req *pb.ConnectRequest) (*pb.ConnectResponse, error) {
	consumer, group, err := g.queue.Connect(
		ctx,
		req.GetConsumerId(),
		req.GetConsumerGroup(),
		req.GetTopics(),
	)
	if err != nil {
		if stdErrors.Is(err, errors.ErrTopicNotFound) {
			return nil, status.Errorf(codes.NotFound, "topics %v not found", req.GetTopics())
		}
		return nil, status.Errorf(codes.Internal, "connect consumer: %v", err)
	}
	return &pb.ConnectResponse{
		Consumer: &pb.Consumer{
			Id:            consumer.ID,
			ConsumerGroup: consumer.ConsumerGroup,
			Partitions:    consumer.Partitions,
			IsActive:      consumer.IsActive,
			Topics:        consumer.Topics,
		},
		ConsumerGroup: &pb.ConsumerGroup{
			Id:        group.ID,
			Consumers: util.Keys(group.Consumers),
			Topics:    util.Keys(group.Topics),
		},
	}, nil
}

func (g *GRPC) Close(ctx context.Context) error {
	g.server.GracefulStop()
	return g.queue.Close(ctx)
}

func (g *GRPC) ShardInfo(ctx context.Context, req *pb.ShardInfoRequest) (*pb.ShardInfoResponse, error) {
	shardsInfo, brokers, leader, err := g.queue.ShardsInfo(ctx, req.GetTopics())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "shard info: %v", err)
	}
	res := &pb.ShardInfoResponse{
		ShardInfo: make(map[string]*pb.ShardInfo),
		Brokers: util.Map(brokers, func(broker *model.Broker) *pb.Broker {
			return &pb.Broker{
				Id:          broker.ID,
				RaftAddress: broker.RaftAddress,
				GrpcAddress: broker.ReachGrpcAddress,
				HttpAddress: broker.ReachHttpAddress,
			}
		}),
		Leader: &pb.Broker{
			Id:          leader.ID,
			RaftAddress: leader.RaftAddress,
			GrpcAddress: leader.ReachGrpcAddress,
			HttpAddress: leader.ReachHttpAddress,
		},
	}
	for partitionID, shardInfo := range shardsInfo {
		shardType := pb.ShardType_SHARD_TYPE_BROKERS
		if shardInfo.ShardType == model.ShardTypePartitions {
			shardType = pb.ShardType_SHARD_TYPE_PARTITIONS
		}
		res.ShardInfo[partitionID] = &pb.ShardInfo{
			ShardId:     shardInfo.ShardID,
			ShardType:   shardType,
			Topic:       shardInfo.Topic,
			PartitionId: shardInfo.PartitionID,
			Brokers: util.Map(shardInfo.Brokers, func(broker *model.Broker) *pb.Broker {
				return &pb.Broker{
					Id:          broker.ID,
					RaftAddress: broker.RaftAddress,
					GrpcAddress: broker.ReachGrpcAddress,
					HttpAddress: broker.ReachHttpAddress,
				}
			}),
		}
	}

	return res, nil
}

func (g *GRPC) ManageBrokers(ctx context.Context, req *pb.ManageBrokersRequest) (*pb.ManageBrokersResponse, error) {
	if req.GetAction() == pb.ManageBrokersAction_MANAGE_BROKERS_ACTION_ADD {
		err := g.queue.RegisterNewNode(
			ctx,
			req.BrokerAction.GetReplicaId(),
			req.BrokerAction.GetRaftAddress(),
		)
		if err != nil {
			if stdErrors.Is(err, errors.ErrCurrentNodeNotLeader) {
				return nil, status.Errorf(
					codes.FailedPrecondition,
					"current node is not the leader",
				)
			} else if stdErrors.Is(err, errors.ErrBrokerAlreadyExists) {
				return nil, status.Errorf(
					codes.AlreadyExists,
					"broker already exists with id %d",
					req.BrokerAction.GetReplicaId(),
				)
			}
			return nil, status.Errorf(codes.Internal, "register new broker: %v", err)
		}
		return nil, nil
	}
	return nil, status.Errorf(codes.Unimplemented, "action %s is not implemented", req.GetAction().String())
}
