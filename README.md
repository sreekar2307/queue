# Queue

A high-performance, fault-tolerant distributed message queue inspired by Kafka — written in Go. It uses Raft (via [Dragonboat](https://github.com/lni/dragonboat)) for consensus, BoltDB for per-partition storage, and features consumer groups with sticky rebalancing.

---

## ✨ Features

### ✅ Core (V0)
- **Topic/Partition model** with persistent on-disk storage (BoltDB per partition)
- **Consumer Groups** with sticky partition assignments
- **At-least-once delivery**
- **gRPC API** for Producer/Consumer interactions
- **Distributed replication using Dragonboat Raft**
- **IOnDiskStateMachine** integration for crash-safe FSM state
- **Heartbeats and Sticky Rebalancing** for dynamic consumer group coordination
- **Manual `Ack` and `Poll` APIs** to control delivery semantics

### 🧠 Under the Hood
- Raft Shards map 1:1 with queue partitions (one Raft group per partition)
- Storage engine is decoupled using interface-based abstractions
- Custom `MessageService` orchestrates routing logic and partition calculations
- No centralized metadata — each broker stores partition state locally

---

## 🚀 Getting Started

### 🛠 Requirements
- Go 1.20
- Protobuf Compiler (`protoc`) if you want to generate gRPC code

### 🔧 Running a Broker

```bash
$ go build -o queue queue
$ ./queue
```
this will start a single broker instance with grpc enabled on 0.0.0.0:8000, check out [transportpb.proto](https://github.com/sreekar2307/queue/blob/main/transport/grpc/transportpb/transport.proto) for the proto definition