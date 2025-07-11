package grpc

import (
	"context"
	stdErrors "errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/sreekar2307/queue/controller"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc/reflection"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc/metadata"

	"buf.build/go/protovalidate"
	provalidateInterceptor "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/sreekar2307/queue/config"
	pb "github.com/sreekar2307/queue/gen/queue/v1"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
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
	ctx context.Context,
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
	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			provalidateInterceptor.UnaryServerInterceptor(pv),
		),
		grpc.ChainStreamInterceptor(
			provalidateInterceptor.StreamServerInterceptor(pv),
		),
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
		Message: msg.ToProtoBuf(),
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
		Message: msg.ToProtoBuf(),
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
	md := metadata.Pairs(TopicMetadataKey, topic.Topic)
	_ = grpc.SetTrailer(ctx, md)
	return &pb.CreateTopicResponse{
		Topic: topic,
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
		Consumer:      consumer,
		ConsumerGroup: group,
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
		ShardInfo: make(map[string]*pbTypes.ShardInfo),
		Brokers: util.Map(brokers, func(broker *model.Broker) *pbTypes.Broker {
			return broker.ToProtoBuf()
		}),
		Leader: leader.ToProtoBuf(),
	}
	for partitionID, shardInfo := range shardsInfo {
		res.ShardInfo[partitionID] = shardInfo.ToProtoBuf()
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
