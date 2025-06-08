# Distributed Queue - gRPC Example

This example demonstrates how to interact with a distributed queue system using gRPC. It showcases:

- Connecting to a 3-node Raft-backed queue cluster
- Creating a topic
- Starting a consumer
- Writing 100 messages to the queue
- Polling and acknowledging messages
- Periodic health checks via a background goroutine

---

## 🛠️ Requirements

- Go 1.24

---

## 🚀 Getting Started

### 1. Install the queue 

```bash
go install github.com/sreekar2307/queue/cmd/queue@v0.1.6
go install github.com/sreekar2307/queue/cmd/proxy@v0.1.6
```

### 2. Start the brokers, each one in a separate terminal

```bash
queue --config config.yaml 
```

```bash
queue --config config.yaml --raft.replica_id 2 --raft.addr localhost:63002 --grpc.listener_addr localhost:8002
```

```bash
queue --config config.yaml --raft.replica_id 3 --raft.addr localhost:63003 --grpc.listener_addr localhost:8003
```

### 2. Start the proxy grpc server 

```bash
proxy
```

### 3. Run the example
```bash
go build -o grpc_client grpc_client && ./grpc_client
```

### 4. Reset the cluster

```bash
rm -r metadata/ partitions/ raft/
```
