# gRPC Proxy for Distributed Queue System

This is a gRPC proxy server used in a distributed queue system to dynamically route incoming gRPC requests to the appropriate broker based on request metadata. It supports metadata-based routing (such as topic or partition IDs) and can optionally force redirection to the leader broker.

---

## ✨ Features

- Transparent proxying of gRPC requests using `grpc-proxy`.
- Metadata-aware routing based on `partition`, `topic`, and `to-leader` headers.
- Connection pooling and reuse for downstream brokers.
- Periodic shard metadata refresh from broker via HTTP or gRPC.
- Lookup backoff retries with exponential delay.

---

## 🛠️ How It Works

1. **Incoming gRPC Request** → Intercepted by the proxy.
2. **Metadata Inspection** → Extracts `partition`, `topic`, and `to-leader` metadata.
3. **Routing Decision** → Determines the appropriate downstream broker using shard info.
4. **Connection Pooling** → Reuses existing gRPC connections via a `sync.Map`.
5. **Transparent Proxying** → Uses `grpc-proxy`'s `TransparentHandler` to forward the call.

---

## 🧾 Required Metadata Headers

| Header                                                          | Description                                                     |
|-----------------------------------------------------------------|-----------------------------------------------------------------|
| `partition`                                                     | Partition ID of the queue (optional).                           |
| `topic`                                                         | Topic name of the queue (optional).                             |
| `to-leader`                                                     | If set to `"true"`, routes to the leader node.                  |

---

## 📁 Internal Components
### director(ctx, method)
Determines the correct broker to route to by reading metadata keys and calling routeToBroker.

### routeToBroker(toLeader, topic, partitionID)
Handles the routing logic:
- Routes to leader if requested. 
- Falls back to brokers by topic or partition. 
- Performs a fresh lookup if information is missing.

### lookupRefresh(ctx)
Runs in background to refresh the clusterDetails periodically or on-demand.

### lookupHttp / lookupGrpc
Responsible for fetching and deserializing shard info either via REST or gRPC.

### sanitizeClusterDetails
Deduplicates brokers per topic and partition based on their ID.

## 🚀 Getting Started

### Run Proxy

```bash
$ go build -o proxy . && ./proxy
```
