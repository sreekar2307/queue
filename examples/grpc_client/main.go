package main

import (
	"context"
	"fmt"
	pb "github.com/sreekar2307/queue/transport/grpc/transportpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"strconv"
	"time"
)

func main() {
	pCtx, pCancel := context.WithCancel(context.Background())
	defer pCancel()
	conn, err := grpc.NewClient("localhost:8000",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()
	client := pb.NewTransportClient(conn)

	writeSomeMessages(pCtx, client)

	// Connect a consumer to a topic
	connectReq := &pb.ConnectRequest{
		ConsumerId:    "node-1",
		ConsumerGroup: "social",
		Topics:        []string{"facebook"},
	}

	ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
	connectResp, err := client.Connect(ctx, connectReq)
	cancel()
	if err != nil {
		log.Fatalf("Connect failed: %v", err)
	}
	runHealthCheck(pCtx, connectResp.Consumer, client)
	readSomeMessages(pCtx, client)
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

func readSomeMessages(pCtx context.Context, client pb.TransportClient) {
	// Create a Transport client
	for range 100 {
		// Receive a message
		recvReq := &pb.ReceiveMessageRequest{
			ConsumerId: "node-1",
		}
		ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
		recvRes, err := client.ReceiveMessage(ctx, recvReq)
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
