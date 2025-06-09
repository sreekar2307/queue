package main

import (
	pb "buf.build/gen/go/sreekar2307/queue/protocolbuffers/go/queue/v1"
	"context"
	"fmt"
	"github.com/sreekar2307/queue/util"
	"log"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"buf.build/gen/go/sreekar2307/queue/grpc/go/queue/v1/v1grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	initialBrokerAddr string
	brokerProxyClient v1grpc.QueueClient
	brokerProxyConn   *grpc.ClientConn
	consumer          *pb.Consumer
}

func NewClient(_ context.Context, initialBrokerAddr string) (*Client, error) {
	c := &Client{
		initialBrokerAddr: initialBrokerAddr,
	}

	initialConn, err := grpc.NewClient(
		c.initialBrokerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	c.brokerProxyConn = initialConn
	c.brokerProxyClient = v1grpc.NewQueueClient(initialConn)

	return c, nil
}

func (c *Client) Close() error {
	if c.brokerProxyConn != nil {
		return c.brokerProxyConn.Close()
	}
	return nil
}

func (c *Client) Start(pCtx context.Context, topics []string, consumerID, consumerGroupID string) error {
	connectReq := &pb.ConnectRequest{
		ConsumerId:    consumerID,
		ConsumerGroup: consumerGroupID,
		Topics:        topics,
	}
	ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
	connectResp, err := c.brokerProxyClient.Connect(ctx, connectReq)
	cancel()
	if err != nil {
		return err
	}
	c.consumer = connectResp.Consumer
	c.runHealthCheck(pCtx)
	return nil
}

func (c *Client) CreateTopic(
	pCtx context.Context,
	topicName string,
	numOfPartitions uint64,
	replicationFactor uint64,
) error {
	createTopicReq := &pb.CreateTopicRequest{
		Topic:              topicName,
		NumberOfPartitions: numOfPartitions,
		ReplicationFactor:  replicationFactor,
	}
	ctx, cancel := context.WithTimeout(pCtx, 30*time.Second)
	_, err := c.brokerProxyClient.CreateTopic(ctx, createTopicReq)
	cancel()
	if err != nil {
		s, _ := status.FromError(err)
		if s.Code() == codes.AlreadyExists {
			return nil
		}
		return err
	}
	return nil
}

func (c *Client) WriteSomeMessages(pCtx context.Context, topic string) error {
	if _, ok := util.FirstMatch(c.consumer.Topics, func(t string) bool {
		return t == topic
	}); !ok {
		return fmt.Errorf("topic %s not found in consumer %s", topic, c.consumer.Id)
	}
	// Send some messages
	for i := range 100 {
		// Prepare the request
		req := &pb.SendMessageRequest{
			Topic:        topic,
			Data:         []byte(fmt.Sprintf("Message No: %d", i)),
			PartitionKey: strconv.Itoa(i),
		}

		// Send the message
		ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("topic", topic))
		_, err := c.brokerProxyClient.SendMessage(ctx, req)
		cancel()
		if err != nil {
			return err
		}
		log.Printf("Message %d sent successfully\n", i)
	}
	log.Println("All messages sent successfully")
	return nil
}

func (c *Client) ReadSomeMessages(pCtx context.Context) error {
	// Create a Transport client
	partitionIDIndex := 0
	partitions := c.consumer.GetPartitions()
label:
	for range 100 {
		// Receive a message
		for j := 0; j < len(partitions); partitionIDIndex, j = (partitionIDIndex+1)%len(partitions), j+1 {
			partitionId := partitions[partitionIDIndex]
			recvReq := &pb.ReceiveMessageForPartitionIDRequest{
				ConsumerId:  c.consumer.Id,
				PartitionId: partitionId,
			}
			ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("partition", partitionId))
			recvRes, err := c.brokerProxyClient.ReceiveMessageForPartitionID(ctx, recvReq)
			cancel()
			if err != nil {
				return err
			}
			log.Println(string(recvRes.GetData()), recvRes.PartitionId, recvRes.MessageId)
			if recvRes.MessageId != nil {
				// Ack the message
				ackReq := &pb.AckMessageRequest{
					ConsumerId:  "node-1",
					PartitionId: recvRes.PartitionId,
					MessageId:   recvRes.MessageId,
				}
				ctx, cancel = context.WithTimeout(pCtx, 5*time.Second)
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("partition", recvRes.PartitionId))
				_, err = c.brokerProxyClient.AckMessage(ctx, ackReq)
				cancel()
				if err != nil {
					return err
				}
				continue label
			}
		}
		break
	}
	return nil
}

func (c *Client) runHealthCheck(pCtx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		select {
		case <-pCtx.Done():
			return
		default:
			// continue
		}
		stream, err := c.brokerProxyClient.HealthCheck(pCtx)
		if err != nil {
			log.Fatalf("failed to create stream: %v", err)
		}
		for {
			select {
			case <-pCtx.Done():
				return
			case <-ticker.C:
				healthCheckReq := &pb.HealthCheckRequest{
					ConsumerId: c.consumer.Id,
					PingAt:     time.Now().In(time.UTC).Unix(),
				}
				if err := stream.Send(healthCheckReq); err != nil {
					log.Fatalf("failed to send health check request: %v", err)
				}
				healthCheckRes, err := stream.Recv()
				if err != nil {
					log.Fatalf("failed to receive health check response: %v", err)
				}
				log.Println("Health check response:", healthCheckRes.GetMessage())
			}
		}
	}()
}
