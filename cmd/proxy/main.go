package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/sreekar2307/queue/transport/grpc/transportpb"
	"github.com/sreekar2307/queue/util"

	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var (
	brokerHttpAddr       = flag.String("broker", "http://0.0.0.0:8081", "http address of the broker")
	brokerGrpcAddr       = flag.String("broker-grpc", "0.0.0.0:8001", "grpc address of the broker")
	proxyAddr            = flag.String("proxy", "0.0.0.0:9000", "Address of the proxy server")
	shardsInfoPath       = flag.String("shards-info-path", "/shards-info", "Path to fetch shard info from the broker")
	shardsInfoReqTimeout = flag.Duration(
		"shards_info_req_timeout",
		5*time.Second,
		"Timeout for fetching shard info from the broker eg (1s, 500ms)",
	)
	minDurationBtwLookups = flag.Duration(
		"min_duration_btw_lookups",
		5*time.Second,
		"Minimum duration between consecutive lookups for shard info eg (5s, 1m)",
	)
	maxLookupRetries = flag.Int("max_lookup_retries", 5, "Maximum number of retries for lookup")

	clusterD        atomic.Pointer[clusterDetails]
	grpcClient      *grpc.ClientConn
	transportClient pb.TransportClient
	lc              = make(chan lookupResource)
)

func main() {
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()

	if len(*brokerHttpAddr) == 0 || len(*brokerGrpcAddr) == 0 {
		log.Fatal("broker address and broker grpc address must be provided")
	}
	if len(*brokerGrpcAddr) != 0 {
		var err error
		grpcClient, err = grpc.NewClient(
			*brokerGrpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("failed to create grpc client: %v", err)
		}
		defer grpcClient.Close()
		transportClient = pb.NewTransportClient(grpcClient)
	}
	go watcher(ctx, lc)

	lc <- lookupResource{} // Trigger initial lookup

	lis, err := net.Listen("tcp", *proxyAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	)

	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// director decides where to forward based on method and metadata
func director(ctx context.Context, _ string) (context.Context, *grpc.ClientConn, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	var (
		partitionID string
		topic       string
	)
	if vals := md.Get("partition"); len(vals) > 0 {
		partitionID = vals[0]
	}
	if vals := md.Get("topic"); len(vals) > 0 {
		topic = vals[0]
	}

	backendAddr, err := routeToBroker(topic, partitionID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to route to broker for partition %s: %w", partitionID, err)
	}
	conn, err := grpc.NewClient(
		backendAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}

	// You can modify metadata here too if needed
	return ctx, conn, nil
}

func watcher(ctx context.Context, lc chan lookupResource) {
	var lastLookupAt *time.Time

	initiateLookup := func(lr lookupResource) {
		log.Println("initiating lookup for cluster details")
		if lastLookupAt == nil || time.Since(*lastLookupAt) >= *minDurationBtwLookups {
			if err := lookupWithBackoff(ctx, lr); err != nil {
				log.Printf("lookup failed: %v", err)
			} else {
				now := time.Now()
				lastLookupAt = &now
			}
		} else {
			log.Println("Skipping lookup, waiting for minimum duration")
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case lr := <-lc:
			initiateLookup(lr)
		case <-time.After(*minDurationBtwLookups):
			initiateLookup(lookupResource{})
		}
	}
}

func routeToBroker(topic, partitionID string) (string, error) {
	log.Println("broker lookup for topic:", topic, "partitionID:", partitionID)
	clusterInfo := clusterD.Load()
	if clusterInfo == nil {
		return "", fmt.Errorf("shard info not available, please retry later")
	}
	if len(clusterInfo.Brokers) == 0 {
		return "", fmt.Errorf("no brokers available")
	}
	var brokers []broker

	if len(partitionID) == 0 && len(topic) == 0 {
		brokers = clusterInfo.Brokers
		return clusterInfo.Brokers[rand.Intn(len(clusterInfo.Brokers))].GrpcAddress, nil
	} else if len(partitionID) == 0 {
		_, ok := clusterInfo.BrokersForTopic[topic]
		if !ok {
			lc <- lookupResource{
				Topic: topic,
			} // Trigger a lookup if topic is not found
		}
		clusterInfo = clusterD.Load()
		brokersForTopic, _ := clusterInfo.BrokersForTopic[topic]
		if len(brokersForTopic) == 0 {
			return "", fmt.Errorf("no brokers available, for topic %s", topic)
		}
		brokers = brokersForTopic
	} else {
		_, ok := clusterInfo.BrokersForPartition[partitionID]
		if !ok {
			lc <- lookupResource{
				Partition: partitionID,
			} // Trigger a lookup if partition is not found
		}
		clusterInfo = clusterD.Load()
		brokersForPartition, _ := clusterInfo.BrokersForPartition[partitionID]
		if len(brokersForPartition) == 0 {
			return "", fmt.Errorf("no brokers available, for partition  %s", partitionID)
		}
		brokers = brokersForPartition
	}

	return brokers[rand.Intn(len(brokers))].GrpcAddress, nil
}

func lookupWithBackoff(ctx context.Context, lr lookupResource) error {
	for i := 0; i < *maxLookupRetries; i++ {
		if err := lookup(ctx); err != nil {
			backoff := durationToBackoff(i, deffaultBackOffConfig)
			time.Sleep(backoff)
			continue
		}
		cd := clusterD.Load()
		if lr.Topic != "" {
			if _, ok := cd.BrokersForTopic[lr.Topic]; !ok {
				backoff := durationToBackoff(i, deffaultBackOffConfig)
				time.Sleep(backoff)
				continue
			}
		}
		if lr.Partition != "" {
			if _, ok := cd.BrokersForPartition[lr.Partition]; !ok {
				backoff := durationToBackoff(i, deffaultBackOffConfig)
				time.Sleep(backoff)
				continue
			}
		}
		log.Println("Successfully fetched shard info")
		return nil
	}
	return fmt.Errorf("failed to fetch shard info after %d retries", *maxLookupRetries)
}

func lookup(pCtx context.Context) error {
	if transportClient != nil {
		return lookupGrpc(pCtx)
	}
	return lookupHttp(pCtx)
}

func lookupGrpc(pCtx context.Context) error {
	ctx, cancel := context.WithTimeout(pCtx, *shardsInfoReqTimeout)
	defer cancel()
	req := &pb.ShardInfoRequest{}
	resp, err := transportClient.ShardInfo(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to fetch shard info: %w", err)
	}
	cd := clusterDetails{
		BrokersForPartition: make(map[string][]broker),
		BrokersForTopic:     make(map[string][]broker),
		Brokers:             make([]broker, 0),
	}
	for partitionID, shardInfo := range resp.ShardInfo {
		brokers := make([]broker, len(shardInfo.Brokers))
		for i, b := range shardInfo.Brokers {
			brokers[i] = broker{
				Id:          b.Id,
				RaftAddress: b.RaftAddress,
				GrpcAddress: b.GrpcAddress,
				HttpAddress: b.HttpAddress,
			}
		}
		cd.BrokersForTopic[shardInfo.Topic] = brokers
		cd.BrokersForPartition[partitionID] = brokers
	}
	cd.Brokers = util.Map(resp.Brokers, func(b *pb.Broker) broker {
		return broker{
			Id:          b.Id,
			RaftAddress: b.RaftAddress,
			GrpcAddress: b.GrpcAddress,
			HttpAddress: b.HttpAddress,
		}
	})
	cd = sanitizeClusterDetails(cd)
	clusterD.Store(&cd)
	return nil
}

func lookupHttp(pCtx context.Context) error {
	ctx, cancel := context.WithTimeout(pCtx, *shardsInfoReqTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet, fmt.Sprintf("%s%s", *brokerHttpAddr, *shardsInfoPath), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch shard info: %s", resp.Status)
	}
	var sr shardsInfo
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return err
	}
	cd := clusterDetails{
		BrokersForPartition: make(map[string][]broker),
		BrokersForTopic:     make(map[string][]broker),
		Brokers:             make([]broker, 0),
	}
	for partitionID, details := range sr.ShardInfo {
		brokers := make([]broker, len(details.Brokers))
		for i, b := range details.Brokers {
			brokers[i] = broker{
				Id:          b.Id,
				RaftAddress: b.RaftAddress,

				GrpcAddress: b.GrpcAddress,
				HttpAddress: b.HttpAddress,
			}
		}
		cd.BrokersForTopic[details.Topic] = brokers
		cd.BrokersForPartition[partitionID] = brokers
	}
	cd.Brokers = sr.Brokers
	cd = sanitizeClusterDetails(cd)
	clusterD.Store(&cd)
	return nil
}

func sanitizeClusterDetails(cd clusterDetails) clusterDetails {
	cd.Brokers = util.UniqBy(cd.Brokers, func(b broker) uint64 {
		return b.Id
	})

	// Ensure brokers for topics and partitions are unique
	for topic, brokers := range cd.BrokersForTopic {
		cd.BrokersForTopic[topic] = util.UniqBy(brokers, func(b broker) uint64 {
			return b.Id
		})
	}
	for partitionID, brokers := range cd.BrokersForPartition {
		cd.BrokersForPartition[partitionID] = util.UniqBy(brokers, func(b broker) uint64 {
			return b.Id
		})
	}

	return cd
}
