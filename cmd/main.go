package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"queue/service"
	"queue/transport/http"
	"syscall"
	"time"
)

func main() {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGKILL, syscall.SIGTERM)
	defer cancel()
	replicaID := flag.Int("replica_id", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	flag.Parse()
	members := map[uint64]string{
		1: "localhost:63001",
		//2: "localhost:63002",
		//3: "localhost:63003",
	}
	trans, err := http.NewTransport(
		ctx,
		service.Config{
			RaftNodeAddr:    *addr,
			ReplicaID:       uint64(*replicaID),
			InviteMembers:   members,
			RaftLogsDataDir: "raft-logs",
			MetadataPath:    "metadata",
			PartitionsPath:  "partitions",
		},
	)
	if err != nil {
		log.Fatalf("failed to create transport: %v", err)
	}
	if err := trans.Start(ctx); err != nil {
		log.Fatalf("failed to start transport: %v", err)
	}
	<-ctx.Done()
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := trans.Close(ctx); err != nil {
		log.Fatalf("failed to close transport: %v", err)
	}
}
