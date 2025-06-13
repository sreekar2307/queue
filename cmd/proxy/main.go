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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/sreekar2307/queue/gen/queue/v1"
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

	clusterD         atomic.Pointer[clusterDetails]
	grpcClient       *grpc.ClientConn
	transportClient  pb.QueueServiceClient
	lc               = make(chan lookupResource)
	lcRes            = make(chan *clusterDetails)
	addrToGrpcClient sync.Map
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
		transportClient = pb.NewQueueServiceClient(grpcClient)
	}
	go lookupRefresh(ctx)

	lc <- lookupResource{} // Trigger initial lookup
	cd := <-lcRes
	clusterD.Store(cd)

	lis, err := net.Listen("tcp", *proxyAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	)

	go func() {
		select {
		case <-ctx.Done():
			addrToGrpcClient.Range(func(key, value any) bool {
				c := value.(*grpc.ClientConn)
				_ = c.Close()
				addrToGrpcClient.Delete(key)
				return true
			})
		}
	}()

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
		toLeader    bool
	)
	if vals := md.Get("partition"); len(vals) > 0 {
		partitionID = vals[0]
	}
	if vals := md.Get("topic"); len(vals) > 0 {
		topic = vals[0]
	}
	if vals := md.Get("to-leader"); len(vals) > 0 && vals[0] == "true" {
		toLeader = true
	}

	conn, err := routeToBroker(toLeader, topic, partitionID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to route to broker for partition %s: %w", partitionID, err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-proxied-by", "grpc-proxy")
	return ctx, conn, nil
}

func routeToBroker(toLeader bool, topic, partitionID string) (*grpc.ClientConn, error) {
	log.Println(
		"broker lookup for topic:",
		topic, "partitionID:",
		partitionID,
		"to-leader:", toLeader,
	)
	clusterInfo := clusterD.Load()
	if clusterInfo == nil {
		return nil, fmt.Errorf("shard info not available, please retry later")
	}
	if len(clusterInfo.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers available")
	}
	var brokers []broker

	createConn := func(addr string) (*grpc.ClientConn, error) {
		conn, ok := addrToGrpcClient.Load(addr)
		if ok {
			return conn.(*grpc.ClientConn), nil
		}
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to create grpc client for address %s: %w", addr, err)
		}
		actual, loaded := addrToGrpcClient.LoadOrStore(addr, cc)
		if loaded {
			_ = cc.Close()
		}
		return actual.(*grpc.ClientConn), nil
	}

	if toLeader {
		return createConn(clusterInfo.LeaderBroker.GrpcAddress)
	}

	if len(partitionID) == 0 && len(topic) == 0 {
		brokers = clusterInfo.Brokers
		if len(brokers) == 0 {
			lc <- lookupResource{} // Trigger a lookup if no brokers are available
			clusterInfo = <-lcRes
			clusterD.Store(clusterInfo)
		}
		if len(clusterInfo.Brokers) == 0 {
			return nil, fmt.Errorf("no brokers available, please retry later")
		}
		brokers = clusterInfo.Brokers
	} else if len(partitionID) == 0 {
		_, ok := clusterInfo.BrokersForTopic[topic]
		if !ok {
			lc <- lookupResource{
				Topic: topic,
			} // Trigger a lookup if topic is not found
			clusterInfo = <-lcRes
			clusterD.Store(clusterInfo)
		}
		brokersForTopic, _ := clusterInfo.BrokersForTopic[topic]
		if len(brokersForTopic) == 0 {
			return nil, fmt.Errorf("no brokers available, for topic %s", topic)
		}
		brokers = brokersForTopic
	} else {
		_, ok := clusterInfo.BrokersForPartition[partitionID]
		if !ok {
			lc <- lookupResource{
				Partition: partitionID,
			} // Trigger a lookup if partition is not found
			clusterInfo = <-lcRes
			clusterD.Store(clusterInfo)
		}
		brokersForPartition, _ := clusterInfo.BrokersForPartition[partitionID]
		if len(brokersForPartition) == 0 {
			return nil, fmt.Errorf("no brokers available, for partition  %s", partitionID)
		}
		brokers = brokersForPartition
	}
	addr := brokers[rand.Intn(len(brokers))].GrpcAddress
	return createConn(addr)
}

func lookupRefresh(ctx context.Context) {
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
			go func() {
				cd := <-lcRes
				clusterD.Store(cd)
			}()
			initiateLookup(lookupResource{})
		}
	}
}

func lookupWithBackoff(ctx context.Context, lr lookupResource) error {
	for i := 0; i < *maxLookupRetries; i++ {
		cd, err := lookup(ctx)
		if err != nil {
			backoff := durationToBackoff(i, deffaultBackOffConfig)
			time.Sleep(backoff)
			continue
		} else {
			lcRes <- cd
		}
		if lr.Topic != "" {
			if _, ok := cd.BrokersForTopic[lr.Topic]; !ok {
				log.Println("brokers for topic not found, retrying lookup", lr.Topic)
				backoff := durationToBackoff(i, deffaultBackOffConfig)
				time.Sleep(backoff)
				continue
			}
		}
		if lr.Partition != "" {
			if _, ok := cd.BrokersForPartition[lr.Partition]; !ok {
				log.Println("brokers not found for partition, retrying lookup", lr.Partition)
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

func lookup(pCtx context.Context) (*clusterDetails, error) {
	if transportClient != nil {
		return lookupGrpc(pCtx)
	}
	return lookupHttp(pCtx)
}

func lookupGrpc(pCtx context.Context) (*clusterDetails, error) {
	ctx, cancel := context.WithTimeout(pCtx, *shardsInfoReqTimeout)
	defer cancel()
	req := &pb.ShardInfoRequest{}
	resp, err := transportClient.ShardInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shard info: %w", err)
	}
	cd := clusterDetails{
		BrokersForPartition: make(map[string][]broker),
		BrokersForTopic:     make(map[string][]broker),
		Brokers:             make([]broker, 0),
		LeaderBroker: broker{
			Id:          resp.Leader.Id,
			RaftAddress: resp.Leader.RaftAddress,
			GrpcAddress: resp.Leader.GrpcAddress,
			HttpAddress: resp.Leader.HttpAddress,
		},
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
	return &cd, nil
}

func lookupHttp(pCtx context.Context) (*clusterDetails, error) {
	ctx, cancel := context.WithTimeout(pCtx, *shardsInfoReqTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet, fmt.Sprintf("%s%s", *brokerHttpAddr, *shardsInfoPath), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch shard info: %s", resp.Status)
	}
	var sr shardsInfo
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, err
	}
	cd := clusterDetails{
		BrokersForPartition: make(map[string][]broker),
		BrokersForTopic:     make(map[string][]broker),
		Brokers:             make([]broker, 0),
		LeaderBroker:        sr.Leader,
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
	return &cd, nil
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
