# Queue

A fault-tolerant distributed message queue inspired by Kafka — written in Go.
It uses Raft (via [Dragonboat](https://github.com/lni/dragonboat)) for consensus,
BoltDB for per-partition storage, and features consumer groups with sticky re-balancing.

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
- Go 1.24
- Protobuf Compiler (`protoc`) if you want to generate gRPC code

### 🔧 Running a Single Node Broker

```bash
$ go build -o queue cmd/queue && ./queue
```
this will start a single broker instance with grpc enabled on 0.0.0.0:8000,
check out [transportpb.proto](https://github.com/sreekar2307/queue/blob/main/transport/grpc/transportpb/transport.proto) for the proto definition


---
### 🧩 Architecture Overview

```text
                        +----------------+
                        |     Client     |
                        | (Producer/Cons)|
                        +-------+--------+
                                |
                                v
                     +----------+----------+
                     |        Broker        |   <-- Handles API (Send, Poll, Ack, etc.)
                     |  (Multiple instances)|   <-- Maps topic-partitions, maintains metadata
                     +----------+----------+
                                |
       +------------------------+-------------------------+
       |                        |                         |
       v                        v                         v
+---------------+      +---------------+         +---------------+
|   Partition 0 |      |   Partition 1 |   ...   |   Partition N |
| (TopicX-P0)   |      | (TopicX-P1)   |         | (TopicY-PM)   |
+------+--------+      +------+--------+         +------+--------+
       |                      |                         |
       | Raft Group (Shard)   | Raft Group (Shard)      | Raft Group (Shard)
       |                      |                         |
+------+------+        +------+------+           +------+------+
| Broker A    |        | Broker B    |           | Broker C    |
| (Leader)    |        | (Leader)    |           | (Leader)    |
| BoltDB P0   |        | BoltDB P1   |           | BoltDB PM   |
+------+------+        +------+------+           +------+------+
       |                      |                         |
+------+------+        +------+------+           +------+------+
| Broker B    |        | Broker C    |           | Broker A    |
| (Follower)  |        | (Follower)  |           | (Follower)  |
+-------------+        +-------------+           +-------------+

                    << Metadata Raft Group >>
                    << Replicates metadata to all brokers >>
+---------------------------------------------------------------+
|          Each broker has its own `metadata.bolt` file         |
|     - Topic definitions                                       |
|     - Partition assignments                                   |
|     - Consumer group membership & offsets                     |
|     Metadata is kept in sync via a Raft shard across brokers  |
+---------------------------------------------------------------+
```


## 🔧 Component Breakdown

### 1. Transport

Handles incoming gRPC and HTTP requests, and translates them into internal commands.

- `grpc/server.go`: gRPC transport layer
- `http/server.go`: Optional HTTP transport layer

---

### 2. Queue

Main orchestration logic:
- Topic and partition creation
- Routing messages
- Interacting with FSMs and services

Acts as the glue between transports, services, and Raft.

---

### 3. FSMs (Finite State Machines)

Implements Dragonboat's `IOnDiskStateMachine`:

- `BrokerFSM`: Tracks broker, topic, consumer metadata 
- `MessageFSM`: Handles message persistence, ack, polling

---

### 4. Service Layer

Domain logic for various operations:

- `BrokerService`: Broker registration, shard info for each partition
- `ConsumerService`: Consumer group registration, offsets, re-balancing
- `TopicService`: Topic creation and introspection
- `MessageService`: Append, poll, ack, etc.

---

### 5. Storage Layer

Each partition’s finite state machine (FSM) persists its data in its own BoltDB file. 
currently hard‑coded the replication factor to three,
so in a three‑node cluster every message in a partition is stored on its leader plus two additional nodes.
Dragonboat takes care of Raft log replication for these partition shards. Separately,
all cluster metadata—topic definitions, consumer group state, and shard membership 
is kept in sync across every broker via a dedicated Raft shard.

---

### 6. Partition Assignors

Implements partition assignment strategies (currently hardcoded to sticky partition assignment):

- Assigns partitions to consumers
- Triggers rebalance on consumer join/leave
- Can be extended with more strategies (e.g., round-robin)

---

## ⚙️ Configuration

This system uses [Viper](https://github.com/spf13/viper) for configuration loading.

### Supported Sources:

- **Flags** (via `pflag`)
- **Environment Variables**
- **All config types supported by viper**
