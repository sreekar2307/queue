package grpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/sreekar2307/queue/config"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service"
	pb "github.com/sreekar2307/queue/transport/grpc/transportpb"
	"github.com/sreekar2307/queue/util"
	"io"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
)

type GRPC struct {
	pb.UnimplementedTransportServer
	queue  *service.Queue
	server *grpc.Server
	config config.GRPC
}

func NewTransport(
	_ context.Context,
	config config.GRPC,
	queue *service.Queue,
) (*GRPC, error) {
	g := &GRPC{
		queue:  queue,
		config: config,
	}

	server := grpc.NewServer()
	server.RegisterService(&pb.Transport_ServiceDesc, g)
	g.server = server
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
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pingAt := time.Unix(req.GetPingAt(), 0).In(time.UTC)

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
		return nil, fmt.Errorf("failed to ack message: %w", err)
	}
	return &pb.AckMessageResponse{}, nil
}

func (g *GRPC) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	msg, err := g.queue.SendMessage(ctx, &model.Message{
		Data:         req.GetData(),
		PartitionKey: req.GetPartitionKey(),
		Topic:        req.GetTopic(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send message: %w", err)
	}
	return &pb.SendMessageResponse{
		Topic:        msg.Topic,
		PartitionKey: msg.PartitionKey,
		Data:         msg.Data,
		PartitionId:  msg.PartitionID,
		MessageId:    msg.ID,
	}, nil
}

func (g *GRPC) ReceiveMessage(ctx context.Context, req *pb.ReceiveMessageRequest) (*pb.ReceiveMessageResponse, error) {
	msg, err := g.queue.ReceiveMessage(ctx, req.GetConsumerId())
	if err != nil {
		return nil, fmt.Errorf("failed to receive message: %w", err)
	}
	if msg == nil {
		return nil, nil
	}
	return &pb.ReceiveMessageResponse{
		Topic:        msg.Topic,
		PartitionKey: msg.PartitionKey,
		Data:         msg.Data,
		PartitionId:  msg.PartitionID,
		MessageId:    msg.ID,
	}, nil
}

func (g *GRPC) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	topic, err := g.queue.CreateTopic(ctx,
		req.GetTopic(),
		req.GetNumberOfPartitions(),
		req.GetReplicationFactor(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create topic: %w", err)
	}
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
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	return &pb.ConnectResponse{
		Consumer: &pb.Consumer{
			Id:            consumer.ID,
			ConsumerGroup: consumer.ConsumerGroup,
			Partitions:    consumer.Partitions,
			IsActive:      consumer.IsActive,
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
