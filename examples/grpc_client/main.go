package main

import (
	"context"
	"log"
)

func main() {
	pCtx, pCancel := context.WithCancel(context.Background())
	defer pCancel()

	client, err := NewClient(pCtx, "0.0.0.0:9000")
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	if err := client.CreateTopic(pCtx, "facebook", 10, 3); err != nil {
		log.Fatalf("failed to create topic: %v", err)
	}
	if err := client.Start(pCtx, []string{"facebook"}, "node-1", "social"); err != nil {
		log.Fatalf("failed to start consumer: %v", err)
	}
	defer client.Close()
	if err := client.WriteSomeMessages(pCtx, "facebook"); err != nil {
		log.Fatalf("failed to write messages: %v", err)
	}

	if err := client.ReadSomeMessages(pCtx); err != nil {
		log.Fatalf("failed to read messages: %v", err)
	}
}
