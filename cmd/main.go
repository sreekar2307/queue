package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"queue"
	"queue/internal/topic"
	"queue/message"
	"syscall"
	"time"
)

func main() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGKILL, syscall.SIGTERM)
	queue := distributedQueue.NewQueue()
	ctx := context.TODO()
	emailTopic, err := queue.CreateTopic(ctx, "emailTopic")
	if err != nil {
		panic(err.Error())
	}
	go receiveMessage(ctx, queue, emailTopic)
	go sendMessage(ctx, queue, emailTopic)
	<-c
}

func sendMessage(ctx context.Context, queue *distributedQueue.Queue, topic *topic.Topic) {
	i := 0
	partitions := []string{
		"userProfile",
		"onboarding",
		"payments",
	}
	for {
		partition := partitions[rand.IntN(len(partitions))]
		msg := message.NewMessage([]byte(fmt.Sprintf("Hello World, %d", i)))
		_, err := queue.SendMessageToPartition(ctx, topic.Name, partition, msg)
		if err != nil {
			panic(err.Error())
		}
		time.Sleep(time.Duration(rand.IntN(3)) * time.Second)
		i++
	}
}

func receiveMessage(ctx context.Context, queue *distributedQueue.Queue, topic *topic.Topic) {
	for {
		msg, err := queue.ReceiveMessage(ctx, topic.Name, "workers")
		if msg == nil {
			continue
		}
		slog.Info("read:", "message", msg)
		if err != nil {
			panic(err.Error())
		}
		if err := queue.AckMessage(ctx, topic.Name, "workers", msg); err != nil {
			panic(err.Error())
		}
		time.Sleep(time.Second)
	}
}
