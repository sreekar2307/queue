package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"queue/model"
	consumerServ "queue/service/consumer"
	topicServ "queue/service/topic"
	"queue/storage"
	"queue/util"
	"slices"
	"strconv"

	"github.com/lni/dragonboat/v4/config"

	"github.com/lni/dragonboat/v4/statemachine"
)

type (
	BrokerFSM struct {
		ShardID           uint64
		ReplicaID         uint64
		mdStorage         storage.MetadataStorage
		topicService      TopicService
		consumerService   ConsumerService
		metaDataStorePath string
		broker            *model.Broker
		partitionsPath    string
	}
)

func NewBrokerFSM(partitionsPath string, broker *model.Broker, mdStorage storage.MetadataStorage) statemachine.CreateOnDiskStateMachineFunc {
	return func(shardID uint64, replicaID uint64) statemachine.IOnDiskStateMachine {
		return &BrokerFSM{
			topicService: topicServ.NewDefaultTopicService(
				mdStorage,
			),
			consumerService: consumerServ.NewDefaultConsumerService(
				mdStorage,
				nil,
			),
			ShardID:        shardID,
			ReplicaID:      replicaID,
			mdStorage:      mdStorage,
			broker:         broker,
			partitionsPath: partitionsPath,
		}
	}
}

func (f *BrokerFSM) Open(stopc <-chan struct{}) (uint64, error) {
	return 0, nil
}

func (f *BrokerFSM) Update(entries []statemachine.Entry) (results []statemachine.Entry, _ error) {
	ctx := context.Background()
	for _, entry := range entries {
		var cmd Cmd
		if err := json.Unmarshal(entry.Cmd, &cmd); err != nil {
			return nil, fmt.Errorf("unmarshing cmd: %w", err)
		}
		if cmd.CommandType == TopicCommands.Create {
			args := cmd.Args
			if len(args) != 2 {
				return nil, fmt.Errorf("invalid command args")
			}
			arg2, err := strconv.ParseUint(string(args[1]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid command args: %w", err)
			}
			topic, err := f.topicService.CreateTopic(ctx, string(args[0]), arg2)
			if err != nil {
				return nil, fmt.Errorf("create topic: %w", err)
			}

			topicBytes, err := json.Marshal(topic)
			if err != nil {
				return nil, fmt.Errorf("marshal topic: %w", err)
			}
			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  topicBytes,
				},
			})
		} else if cmd.CommandType == PartitionsCommands.PartitionAdded {
			args := cmd.Args
			if len(args) != 2 {
				return nil, fmt.Errorf("invalid command args")
			}
			shardID, err := strconv.ParseUint(string(args[0]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid command args: %w", err)
			}
			members := make(map[uint64]string)
			if err := json.Unmarshal(args[1], &members); err != nil {
				return nil, fmt.Errorf("unmarshing cmd: %w", err)
			}
			_, ok := util.FirstMatch(util.Keys(members), func(k uint64) bool {
				return k == f.broker.ID
			})
			if !ok {
				results = append(results, statemachine.Entry{
					Index: entry.Index,
					Cmd:   slices.Clone(entry.Cmd),
					Result: statemachine.Result{
						Value: entry.Index,
						Data:  nil,
					},
				})
				continue
			}
			nh := f.broker.NodeHost()
			err = nh.StartOnDiskReplica(members, false, NewMessageFSM(f.partitionsPath, f.mdStorage), config.Config{
				ReplicaID:          f.broker.ID,
				ShardID:            shardID,
				ElectionRTT:        10,
				HeartbeatRTT:       1,
				CheckQuorum:        true,
				SnapshotEntries:    1000,
				CompactionOverhead: 50,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to start replica: %w", err)
			}
			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  nil,
				},
			})
		}
	}
	return results, nil
}

func (f *BrokerFSM) Lookup(i any) (any, error) {
	var (
		cmd Cmd
		ctx = context.Background()
	)
	if err := json.Unmarshal(i.([]byte), &cmd); err != nil {
		return nil, fmt.Errorf("unmarshing cmd: %w", err)
	}
	if cmd.CommandType == TopicCommands.Get {
		args := cmd.Args
		if len(args) != 1 {
			return nil, fmt.Errorf("invalid command args")
		}
		topic, err := f.topicService.GetTopic(ctx, string(args[0]))
		if err != nil {
			return nil, fmt.Errorf("get topic: %w", err)
		}
		topicBytes, err := json.Marshal(topic)
		if err != nil {
			return nil, fmt.Errorf("marshal topic: %w", err)
		}
		return topicBytes, nil
	} else if cmd.CommandType == PartitionsCommands.GetPartitions {
		args := cmd.Args
		if len(args) != 1 {
			return nil, fmt.Errorf("invalid command args")
		}
		partitions, err := f.topicService.GetPartitions(ctx, string(args[0]))
		if err != nil {
			return nil, fmt.Errorf("get partitions: %w", err)
		}
		partitionsBytes, err := json.Marshal(partitions)
		if err != nil {
			return nil, fmt.Errorf("marshal partitions: %w", err)
		}
		return partitionsBytes, nil
	} else if cmd.CommandType == PartitionsCommands.PartitionID {

		args := cmd.Args
		if len(args) != 1 {
			return nil, fmt.Errorf("invalid command args")
		}
		var msg model.Message
		if err := json.Unmarshal(args[0], &msg); err != nil {
			return nil, fmt.Errorf("unmarshing cmd: %w", err)
		}
		partitionID, err := f.topicService.PartitionID(ctx, &msg)
		if err != nil {
			return nil, fmt.Errorf("get partitionID: %w", err)
		}
		return []byte(partitionID), nil
	}
	return nil, fmt.Errorf("invalid command type")
}

func (f *BrokerFSM) Sync() error {
	return nil
}

func (f *BrokerFSM) PrepareSnapshot() (any, error) {
	// TODO implement me
	panic("implement me")
}

func (f *BrokerFSM) SaveSnapshot(i any, writer io.Writer, i2 <-chan struct{}) error {
	// TODO implement me
	panic("implement me")
}

func (f *BrokerFSM) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	// TODO implement me
	panic("implement me")
}

func (f *BrokerFSM) Close() error {
	return nil
}
