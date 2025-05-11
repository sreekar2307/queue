package main

import (
	"context"
	stdErrors "errors"
	"fmt"
	"log"
	"strconv"
	"time"

	queueErrors "github.com/sreekar2307/queue/service/errors"
	pb "github.com/sreekar2307/queue/transport/grpc/transportpb"
	"github.com/sreekar2307/queue/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	initialBrokerAddr    string
	initialBrokerClient  pb.TransportClient
	initialBrokerConn    *grpc.ClientConn
	consumer             *pb.Consumer
	clientsForPartitions map[string]pb.TransportClient
	connForPartitions    map[string]*grpc.ClientConn
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
	c.initialBrokerConn = initialConn
	c.initialBrokerClient = pb.NewTransportClient(initialConn)

	return c, nil
}

func (c *Client) Close() error {
	if c.initialBrokerConn != nil {
		return c.initialBrokerConn.Close()
	}

	for _, conn := range c.connForPartitions {
		if conn != nil {
			if err := conn.Close(); err != nil {
				return err
			}
		}
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
	connectResp, err := c.initialBrokerClient.Connect(ctx, connectReq)
	cancel()
	if err != nil {
		return err
	}
	c.consumer = connectResp.Consumer
	c.runHealthCheck(pCtx)
	if err := c.createTransportClientForPartition(pCtx); err != nil {
		if stdErrors.Is(err, queueErrors.ErrTopicAlreadyExists) {
			return nil
		}
		return err
	}
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
	_, err := c.initialBrokerClient.CreateTopic(ctx, createTopicReq)
	cancel()
	if err != nil {
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
		_, err := c.initialBrokerClient.SendMessage(ctx, req)
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
			client, ok := c.clientsForPartitions[partitionId]
			if !ok {
				return fmt.Errorf("no client found for partition %s", partitionId)
			}
			ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
			recvRes, err := client.ReceiveMessageForPartitionID(ctx, recvReq)
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
				_, err = client.AckMessage(ctx, ackReq)
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
		stream, err := c.initialBrokerClient.HealthCheck(pCtx)
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

func (c *Client) createTransportClientForPartition(
	pCtx context.Context,
) error {
	ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
	defer cancel()
	shardInfoPerPartition, err := c.initialBrokerClient.ShardInfo(
		ctx,
		&pb.ShardInfoRequest{
			Topics: c.consumer.Topics,
		},
	)
	if err != nil {
		return err
	}

	clientForPartition := make(map[string]pb.TransportClient)

	conns := make(map[string]*grpc.ClientConn)
	for partitionId, shardInfo := range shardInfoPerPartition.GetShardInfo() {
		brokers := shardInfo.Brokers
		if len(brokers) == 0 {
			return fmt.Errorf("no brokers found for partition %s", partitionId)
		}
		addr := brokers[0].GetGrpcAddress()
		if conn, ok := conns[addr]; ok {
			client := pb.NewTransportClient(conn)
			clientForPartition[partitionId] = client
			continue
		}
		conn, err := grpc.NewClient(
			brokers[0].GetGrpcAddress(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return err
		}
		conns[addr] = conn
		go func() {
			<-pCtx.Done()
			_ = conn.Close()
		}()
		client := pb.NewTransportClient(conn)
		clientForPartition[partitionId] = client
	}
	c.clientsForPartitions = clientForPartition
	c.connForPartitions = conns
	return nil
}
