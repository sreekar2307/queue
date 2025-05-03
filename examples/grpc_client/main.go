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
	ctx := context.Background()
	writeSomeMessages(ctx)
	readSomeMessages(ctx)
}

func writeSomeMessages(ctx context.Context) {
	conn, err := grpc.NewClient("localhost:8000",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Create a Transport client
	client := pb.NewTransportClient(conn)

	// Create a topic
	createTopicReq := &pb.CreateTopicRequest{
		Topic:              "facebook",
		NumberOfPartitions: 10,
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, err = client.CreateTopic(ctx, createTopicReq)
	cancel()
	if err != nil {
		log.Fatalf("CreateTopic failed: %v", err)
	}

	// Send some messages
	for i := range 100 {
		// Prepare the request
		req := &pb.SendMessageRequest{
			Topic:        "facebook",
			Data:         []byte(fmt.Sprintf("sreekar bollam %d", i)),
			PartitionKey: strconv.Itoa(i),
		}

		// Set a context with timeout
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		// Call SendMessage
		_, err = client.SendMessage(ctx, req)
		cancel()
		if err != nil {
			log.Fatalf("SendMessage failed: %v", err)
		}
	}
}

func readSomeMessages(pCtx context.Context) {
	conn, err := grpc.NewClient("localhost:8000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Connect a consumer to a topic
	connectReq := &pb.ConnectRequest{
		ConsumerId:    "node-1",
		ConsumerGroup: "social",
		Topics:        []string{"facebook"},
	}
	client := pb.NewTransportClient(conn)
	ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
	_, err = client.Connect(ctx, connectReq)
	cancel()

	// Create a Transport client
	for range 100 {
		// Receive a message
		recvReq := &pb.ReceiveMessageRequest{
			ConsumerId: "node-1",
		}
		ctx, cancel = context.WithTimeout(pCtx, 5*time.Second)
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
}
