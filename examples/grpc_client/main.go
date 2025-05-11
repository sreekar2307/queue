package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	pb "github.com/sreekar2307/queue/transport/grpc/transportpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	pCtx, pCancel := context.WithCancel(context.Background())
	defer pCancel()
	// start the connection with any one broker
	initialConn, err := grpc.NewClient(
		"localhost:8000",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer func() {
		_ = initialConn.Close()
	}()
	initialClient := pb.NewTransportClient(initialConn)

	// writeSomeMessages(pCtx, initialClient)

	// Connect a consumer to a topic
	topics := []string{"facebook"}
	connectReq := &pb.ConnectRequest{
		ConsumerId:    "node-1",
		ConsumerGroup: "social",
		Topics:        topics,
	}

	ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
	connectResp, err := initialClient.Connect(ctx, connectReq)
	cancel()
	if err != nil {
		log.Fatalf("Connect failed: %v", err)
	}
	sharInfoPerPartition := shardsInfo(
		pCtx,
		topics,
		connectResp.Consumer,
		initialClient,
	)
	clientForPartition := createClientForPartition(
		pCtx,
		sharInfoPerPartition,
	)

	runHealthCheck(pCtx, connectResp.Consumer, initialClient)
	readSomeMessages(pCtx, clientForPartition)
	pCancel()
}

func writeSomeMessages(pCtx context.Context, client pb.TransportClient) {
	// Create a topic
	createTopicReq := &pb.CreateTopicRequest{
		Topic:              "facebook",
		NumberOfPartitions: 10,
	}
	ctx, cancel := context.WithTimeout(pCtx, 30*time.Second)
	_, err := client.CreateTopic(ctx, createTopicReq)
	cancel()
	if err != nil {
		log.Fatalf("CreateTopic failed: %v", err)
	}
	log.Println("Topic created successfully")

	// Send some messages
	for i := range 100 {
		// Prepare the request
		req := &pb.SendMessageRequest{
			Topic:        "facebook",
			Data:         []byte(fmt.Sprintf("Message No: %d", i)),
			PartitionKey: strconv.Itoa(i),
		}

		// Send the message
		ctx, cancel = context.WithTimeout(pCtx, 5*time.Second)
		_, err := client.SendMessage(ctx, req)
		cancel()
		if err != nil {
			log.Fatalf("SendMessage failed: %v", err)
		}
		log.Printf("Message %d sent successfully\n", i)
	}
	log.Println("All messages sent successfully")
}

func readSomeMessages(pCtx context.Context, clients map[string]pb.TransportClient) {
	// Create a Transport client
	for range 100 {
		// Receive a message
		recvReq := &pb.ReceiveMessageRequest{
			ConsumerId: "node-1",
		}
		ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
		recvRes, err := clients[""].ReceiveMessage(ctx, recvReq)
		cancel()
		if err != nil {
			log.Fatalf("SendMessage failed: %v", err)
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
			client, ok := clients[recvRes.PartitionId]
			if !ok {
				log.Fatalf("No client found for partition %s", recvRes.PartitionId)
			}
			_, err = client.AckMessage(ctx, ackReq)
			cancel()
			if err != nil {
				log.Fatalf("AckMessage failed: %v", err)
			}
		}
	}
	log.Println("All messages received successfully")
}

func runHealthCheck(pCtx context.Context, consumer *pb.Consumer, client pb.TransportClient) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		stream, err := client.HealthCheck(pCtx)
		if err != nil {
			log.Fatalf("failed to create stream: %v", err)
		}
		for {
			select {
			case <-pCtx.Done():
				return
			case <-ticker.C:
				healthCheckReq := &pb.HealthCheckRequest{
					ConsumerId: consumer.Id,
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

func shardsInfo(pCtx context.Context,
	topics []string,
	consumer *pb.Consumer, client pb.TransportClient,
) map[string]*pb.ShardInfo {
	shardInfo, err := client.ShardInfo(
		pCtx,
		&pb.ShardInfoRequest{
			Topics: topics,
		},
	)
	if err != nil {
		log.Fatalf("failed to get shard info: %v", err)
	}
	return shardInfo.ShardInfo
}

func createClientForPartition(
	pCtx context.Context,
	shardInfoPerPartition map[string]*pb.ShardInfo,
) map[string]pb.TransportClient {
	clientForPartition := make(map[string]pb.TransportClient)

	for partitionId, shardInfo := range shardInfoPerPartition {
		brokers := shardInfo.Brokers
		if len(brokers) == 0 {
			log.Fatalf("no brokers found for partition %s", partitionId)
		}
		initialConn, err := grpc.NewClient(
			brokers[0].GetGrpcAddress(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}
		defer func() {
			_ = initialConn.Close()
		}()
		client := pb.NewTransportClient(initialConn)
		clientForPartition[partitionId] = client
	}
	return clientForPartition
}
