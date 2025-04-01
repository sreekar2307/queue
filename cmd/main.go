package main

import (
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
	var c = make(chan os.Signal)
	signal.Notify(c, syscall.SIGKILL, syscall.SIGTERM)
	queue := distributedQueue.NewQueue()
	emailTopic, err := queue.CreateTopic("emailTopic")
	if err != nil {
		panic(err.Error())
	}
	go receiveMessage(queue, emailTopic)
	go sendMessage(queue, emailTopic)
	<-c
}

func sendMessage(queue *distributedQueue.Queue, topic *topic.Topic) {
	i := 0
	partitions := []string{
		"userProfile",
		"onboarding",
		"payments",
	}
	for {
		partition := partitions[rand.IntN(len(partitions))]
		msg := message.NewMessage([]byte(fmt.Sprintf("Hello World, %d", i)))
		_, err := queue.SendMessageToPartition(topic.Name, partition, msg)
		if err != nil {
			panic(err.Error())
		}
		time.Sleep(time.Duration(rand.IntN(3)) * time.Second)
		i++
	}
}

func receiveMessage(queue *distributedQueue.Queue, topic *topic.Topic) {
	for {
		msg, err := queue.ReceiveMessage(topic.Name, "workers")
		if msg == nil {
			continue
		}
		slog.Info("read:", "message", msg)
		if err != nil {
			panic(err.Error())
		}
		if err := queue.AckMessage(topic.Name, "workers", msg); err != nil {
			panic(err.Error())
		}
		time.Sleep(time.Second)
	}
}
