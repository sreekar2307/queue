package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"queue/model"
	"queue/transport/embedded"
	"syscall"
)

func main() {
	ctx := context.Background()
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGKILL, syscall.SIGTERM)
	replicaID := flag.Int("replica_id", 1, "ReplicaID to use")
	join := flag.Bool("join", false, "Joining a new node")
	addr := flag.String("addr", "", "Nodehost address")
	flag.Parse()
	members := map[uint64]string{
		1: "localhost:63001",
		2: "localhost:63002",
		3: "localhost:63003",
	}
	trans, err := embedded.NewTransport(
		ctx,
		*addr,
		uint64(*replicaID),
		members,
		"raft-logs",
	)
	if err != nil {
		panic(err.Error())
	}
	if !*join {
		topic, err := trans.CreateTopic(ctx, "facebookTopic", 3)
		if err != nil {
			panic(err.Error())
		}
		log.Println("Topic created:", "topic", topic)
		for i := range 500 {
			msg := &model.Message{
				TopicName: topic.Name,
				Data:      fmt.Appendf(nil, "Hello from world %d", i),
			}
			msg, err = trans.SendMessage(ctx, msg)
			if err != nil {
				panic(err.Error())
			}
			log.Println("Message sent:", "message", msg)
		}
		log.Println("all messages sent")
	}
	//listKeys()
	<-c
	if err := trans.Close(ctx); err != nil {
		log.Printf("failed to close transport %s\n", err.Error())
	}
}

func listKeys() {
	// Replace with the path to your BoltDB file
	dbPaths := []string{
		"partitions/2/1/facebookTopic-0",
		"partitions/3/1/facebookTopic-1",
		"partitions/4/1/facebookTopic-2",
	}

	for _, dbPath := range dbPaths {
		db, err := bolt.Open(dbPath, 0600, &bolt.Options{
			ReadOnly: true,
		})
		if err != nil {
			log.Fatalf("Failed to open BoltDB file: %v", err)
		}
		defer db.Close()
		err = db.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(bucketName []byte, b *bolt.Bucket) error {
				//fmt.Printf("Bucket: %s\n", bucketName)

				err := b.ForEach(func(k, v []byte) error {
					// If value is nil, it's a nested bucket
					if v == nil {
						log.Println("Nested Bucket", "key", k)
					} else {
						log.Println("key, value", "key", binary.BigEndian.Uint64(k), "value", string(v))
					}
					return nil
				})
				if err != nil {
					return fmt.Errorf("error iterating bucket %s: %v", bucketName, err)
				}
				return nil
			})
		})
		if err != nil {
			slog.Error("error reading BoltDB", "error", err)
		}
	}
}
