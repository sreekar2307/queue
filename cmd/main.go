package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"queue/config"
	"queue/service"
	"queue/transport"
	"queue/transport/grpc"
	"syscall"
	"time"
)

func main() {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGKILL, syscall.SIGTERM)
	defer cancel()
	replicaID := flag.Int("replica_id", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	startGRPC := flag.Bool("grpc", false, "Pass this flag to start GPRC")
	flag.Parse()
	members := map[uint64]string{
		1: "localhost:63001",
		//2: "localhost:63002",
		//3: "localhost:63003",
	}
	conf := config.Config{
		RaftConfig: &config.RaftConfig{
			RaftNodeAddr:    *addr,
			ReplicaID:       uint64(*replicaID),
			InviteMembers:   members,
			RaftLogsDataDir: "raft-logs",
		},
		MetadataPath:   "metadata",
		PartitionsPath: "partitions",
		GRPC: &config.GRPCConfig{
			ListenerAddr: ":8000",
		},
	}
	queue, err := service.NewQueue(ctx, conf)
	if err != nil {
		log.Fatalf("failed to create queue: %v", err)
	}
	var trans transport.Transport
	if *startGRPC {
		if conf.GRPC == nil {
			log.Fatalf("GRPC config is nil")
		}
		trans, err = grpc.NewTransport(
			ctx,
			conf.GRPC,
			queue,
		)
		if err != nil {
			log.Fatalf("failed to create transport: %v", err)
		}
		if err := trans.Start(ctx); err != nil {
			log.Fatalf("failed to start transport: %v", err)
		}
	}
	<-ctx.Done()
	if trans != nil {
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := trans.Close(ctx); err != nil {
			log.Fatalf("failed to close transport: %v", err)
		}
	}
}
