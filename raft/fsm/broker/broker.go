package broker

import (
	"context"
	"encoding/json"
	stdErrors "errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"io"
	"log"
	"slices"
	"strconv"

	"github.com/sreekar2307/queue/assignor/sticky"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"github.com/sreekar2307/queue/raft/fsm/command/factory"
	"github.com/sreekar2307/queue/raft/fsm/message"
	"github.com/sreekar2307/queue/service"
	brokerServ "github.com/sreekar2307/queue/service/broker"
	consumerServ "github.com/sreekar2307/queue/service/consumer"
	topicServ "github.com/sreekar2307/queue/service/topic"

	"github.com/sreekar2307/queue/config"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service/errors"
	"github.com/sreekar2307/queue/storage"
	"github.com/sreekar2307/queue/util"

	drConfig "github.com/lni/dragonboat/v4/config"

	"github.com/lni/dragonboat/v4/statemachine"
)

type (
	fsm struct {
		shardID           uint64
		ReplicaID         uint64
		mdStorage         storage.MetadataStorage
		topicService      service.TopicService
		consumerService   service.ConsumerService
		brokerService     service.BrokerService
		metaDataStorePath string
		broker            *model.Broker
	}
)

func NewBrokerFSM(
	shardID, replicaID uint64,
	broker *model.Broker,
	mdStorage storage.MetadataStorage,
) statemachine.IOnDiskStateMachine {
	return &fsm{
		topicService: topicServ.NewDefaultTopicService(
			mdStorage,
		),
		consumerService: consumerServ.NewDefaultConsumerService(
			mdStorage,
			sticky.NewAssignor(mdStorage),
		),
		brokerService: brokerServ.NewDefaultBrokerService(
			mdStorage,
		),
		shardID:   shardID,
		ReplicaID: replicaID,
		mdStorage: mdStorage,
		broker:    broker,
	}
}

func (f *fsm) TopicService() service.TopicService {
	return f.topicService
}

func (f *fsm) ConsumerService() service.ConsumerService {
	return f.consumerService
}

func (f *fsm) BrokerService() service.BrokerService {
	return f.brokerService
}

func (f *fsm) Broker() *model.Broker {
	return f.broker
}

func (f *fsm) Open(_ <-chan struct{}) (uint64, error) {
	ctx := context.Background()
	commandID, err := f.topicService.LastAppliedCommandID(ctx, f.shardID)
	if err != nil {
		return 0, fmt.Errorf("get last applied command ID: %w", err)
	}
	return commandID, nil
}

func (f *fsm) Update(entries []statemachine.Entry) (results []statemachine.Entry, _ error) {
	ctx := context.Background()
	for _, entry := range entries {
		var (
			cmd   command.Cmd
			cmdV2 pbCommandTypes.Cmd
		)
		if err := json.Unmarshal(entry.Cmd, &cmd); err != nil {
			if err := proto.Unmarshal(entry.Cmd, &cmdV2); err != nil {
				return nil, fmt.Errorf("unmarshing cmd: %w", err)
			}
		}
		log.Println("Processing command", cmd.CommandType, "with args", cmd.Args,
			"at index", entry.Index, "for broker fsm")
		if cmd.CommandType == command.TopicCommands.CreateTopic || cmdV2.Cmd == pbCommandTypes.Kind_KIND_CREATE_TOPIC {
			updator, err := factory.BrokerExecuteUpdate(cmdV2.Cmd, f)
			if err != nil {
				return nil, fmt.Errorf("get update for command %s: %w", cmd.CommandType, err)
			}
			resEntry, err := updator.ExecuteUpdate(ctx, cmdV2.Args, entry)
			if err != nil {
				return nil, fmt.Errorf("execute update for command %s: %w", cmd.CommandType, err)
			}
			results = append(results, resEntry)
		} else if cmd.CommandType == command.PartitionsCommands.PartitionAdded {
			args := cmd.Args
			if len(args) != 3 {
				return nil, fmt.Errorf("invalid command args")
			}
			partitionID := string(args[0])
			shardID, err := strconv.ParseUint(string(args[1]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid command args: %w", err)
			}
			members := make(map[uint64]string)
			if err := json.Unmarshal(args[2], &members); err != nil {
				return nil, fmt.Errorf("unmarshing cmd: %w", err)
			}
			partitionUpdates := &model.Partition{
				Members: members,
				ShardID: shardID,
			}
			if err := f.topicService.UpdatePartition(ctx, entry.Index, partitionID, partitionUpdates); err != nil {
				if stdErrors.Is(err, errors.ErrDuplicateCommand) {
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
				return nil, fmt.Errorf("update partition: %w", err)
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
			f.broker.AddShardIDForPartitionID(partitionID, shardID)
			nh := f.broker.NodeHost()
			log.Println("Starting replica for partition", partitionID, "on shard", shardID,
				"replicaID", f.broker.ID)
			raftConfig := config.Conf.RaftConfig
			err = nh.StartOnDiskReplica(members, false, func(shardID, replicaID uint64) statemachine.IOnDiskStateMachine {
				return message.NewMessageFSM(
					shardID,
					replicaID,
					f.broker,
					f.mdStorage,
				)
			}, drConfig.Config{
				ReplicaID:          f.broker.ID,
				ShardID:            shardID,
				ElectionRTT:        10,
				HeartbeatRTT:       1,
				CheckQuorum:        true,
				SnapshotEntries:    raftConfig.Messages.SnapshotsEntries,
				CompactionOverhead: raftConfig.Messages.CompactionOverhead,
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
		} else if cmd.CommandType == command.ConsumerCommands.Connect {
			args := cmd.Args
			if len(args) != 3 {
				return nil, fmt.Errorf("invalid command args")
			}
			consumerGroupID := string(args[0])
			consumerID := string(args[1])
			topics := make([]string, 0)
			if err := json.Unmarshal(args[2], &topics); err != nil {
				return nil, fmt.Errorf("unmarshing cmd: %w", err)
			}
			consumer, consumerGroup, err := f.consumerService.Connect(
				ctx,
				entry.Index,
				consumerGroupID,
				consumerID,
				topics,
			)
			if err != nil {
				if stdErrors.Is(err, errors.ErrDuplicateCommand) {
					results = append(results, statemachine.Entry{
						Index: entry.Index,
						Cmd:   slices.Clone(entry.Cmd),
						Result: statemachine.Result{
							Value: entry.Index,
							Data:  nil,
						},
					})
					continue
				} else if stdErrors.Is(err, errors.ErrTopicNotFound) {
					res := struct {
						TopicNotFound bool
					}{
						TopicNotFound: true,
					}
					resultBytes, err := json.Marshal(res)
					if err != nil {
						return nil, fmt.Errorf("marshal consumer: %w", err)
					}
					results = append(results, statemachine.Entry{
						Index: entry.Index,
						Cmd:   slices.Clone(entry.Cmd),
						Result: statemachine.Result{
							Value: entry.Index,
							Data:  resultBytes,
						},
					})
					continue
				}
				return nil, fmt.Errorf("create consumer: %w", err)
			}
			res := struct {
				Consumer      *model.Consumer
				Group         *model.ConsumerGroup
				TopicNotFound bool
			}{
				Consumer:      consumer,
				Group:         consumerGroup,
				TopicNotFound: false,
			}
			resultBytes, err := json.Marshal(res)
			if err != nil {
				return nil, fmt.Errorf("marshal consumer: %w", err)
			}

			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  resultBytes,
				},
			})
		} else if cmd.CommandType == command.ConsumerCommands.Disconnected {
			args := cmd.Args
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid command args")
			}
			consumerID := string(args[0])
			err := f.consumerService.Disconnect(ctx, entry.Index, consumerID)
			if err != nil {
				if stdErrors.Is(err, errors.ErrDuplicateCommand) {
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
				return nil, fmt.Errorf("create consumer: %w", err)
			}
			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  nil,
				},
			})
		} else if cmd.CommandType == command.ConsumerCommands.HealthCheck {
			args := cmd.Args
			if len(args) != 2 {
				return nil, fmt.Errorf("invalid command args")
			}
			consumerID := string(args[0])
			lastHealthCheckAt, err := strconv.ParseInt(string(args[1]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid command args: %w", err)
			}
			consumer, err := f.consumerService.HealthCheck(ctx, entry.Index, consumerID, lastHealthCheckAt)
			if err != nil {
				if stdErrors.Is(err, errors.ErrDuplicateCommand) {
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
				return nil, fmt.Errorf("create consumer: %w", err)
			}
			consumerBytes, err := json.Marshal(consumer)
			if err != nil {
				return nil, fmt.Errorf("marshal consumer: %w", err)
			}
			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  consumerBytes,
				},
			})
		} else if cmd.CommandType == command.ConsumerCommands.UpdateConsumer {
			args := cmd.Args
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid command args")
			}
			var consumer model.Consumer
			if err := json.Unmarshal(args[0], &consumer); err != nil {
				return nil, fmt.Errorf("unmarshing cmd: %w", err)
			}
			updatedConsumer, err := f.consumerService.UpdateConsumer(ctx, entry.Index, &consumer)
			if err != nil {
				if stdErrors.Is(err, errors.ErrDuplicateCommand) {
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
				return nil, fmt.Errorf("update consumer: %w", err)
			}
			consumerBytes, err := json.Marshal(updatedConsumer)
			if err != nil {
				return nil, fmt.Errorf("marshal consumer: %w", err)
			}
			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  consumerBytes,
				},
			})
		} else if cmd.CommandType == command.BrokerCommands.RegisterBroker {
			args := cmd.Args
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid command args")
			}
			var broker model.Broker
			if err := json.Unmarshal(args[0], &broker); err != nil {
				return nil, fmt.Errorf("unmarshing cmd: %w", err)
			}
			_, err := f.brokerService.RegisterBroker(ctx, entry.Index, &broker)
			if err != nil {
				if !stdErrors.Is(err, errors.ErrDuplicateCommand) {
					return nil, fmt.Errorf("register broker: %w", err)
				}
			}
			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  nil,
				},
			})

		} else {
			return nil, fmt.Errorf("invalid command type: %s", cmd.CommandType)
		}
	}
	return results, nil
}

func (f *fsm) Lookup(i any) (any, error) {
	var (
		cmd   command.Cmd
		cmdV2 pbCommandTypes.Cmd
		ctx   = context.Background()
	)
	if err := json.Unmarshal(i.([]byte), &cmd); err != nil {
		if err := proto.Unmarshal(i.([]byte), &cmdV2); err != nil {
			return nil, fmt.Errorf("unmarshing cmd: %w", err)
		}
	}
	if cmd.CommandType == command.TopicCommands.TopicForID || cmdV2.Cmd == pbCommandTypes.Kind_KIND_TOPIC_FOR_ID {
		lookup, err := factory.BrokerLookup(cmdV2.Cmd, f)
		if err != nil {
			return nil, fmt.Errorf("get lookup for command %s: %w", cmd.CommandType, err)
		}
		resEntry, err := lookup.Lookup(ctx, cmdV2.Args)
		if err != nil {
			return nil, fmt.Errorf("execute lookup for command %s: %w", cmd.CommandType, err)
		}
		return resEntry, nil
	} else if cmd.CommandType == command.PartitionsCommands.PartitionsForTopic {
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
	} else if cmd.CommandType == command.PartitionsCommands.PartitionForID {
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
	} else if cmd.CommandType == command.PartitionsCommands.AllPartitions {
		partitions, err := f.topicService.AllPartitions(ctx)
		if err != nil {
			return nil, fmt.Errorf("get partitions: %w", err)
		}
		partitionsBytes, err := json.Marshal(partitions)
		if err != nil {
			return nil, fmt.Errorf("marshal partitions: %w", err)
		}
		return partitionsBytes, nil
	} else if cmd.CommandType == command.ConsumerCommands.ConsumerForID {
		if len(cmd.Args) != 1 {
			return nil, fmt.Errorf("invalid command args")
		}
		consumer, err := f.consumerService.GetConsumer(ctx, string(cmd.Args[0]))
		if err != nil {
			return nil, fmt.Errorf("get consumer: %w", err)
		}
		consumerBytes, err := json.Marshal(consumer)
		if err != nil {
			return nil, fmt.Errorf("marshal consumer: %w", err)
		}
		return consumerBytes, nil

	} else if cmd.CommandType == command.ConsumerCommands.Consumers {
		if len(cmd.Args) != 0 {
			return nil, fmt.Errorf("invalid command args")
		}
		consumers, err := f.consumerService.AllConsumers(ctx)
		if err != nil {
			return nil, fmt.Errorf("get consumers: %w", err)
		}
		consumersBytes, err := json.Marshal(consumers)
		if err != nil {
			return nil, fmt.Errorf("marshal consumers: %w", err)
		}
		return consumersBytes, nil
	} else if cmd.CommandType == command.BrokerCommands.ShardInfoForPartitions {
		if len(cmd.Args) != 1 {
			return nil, fmt.Errorf("invalid command args")
		}
		partitions := make([]*model.Partition, 0)
		if err := json.Unmarshal(cmd.Args[0], &partitions); err != nil {
			return nil, fmt.Errorf("unmarshing cmd: %w", err)
		}
		shardInfo, brokers, err := f.brokerService.ShardInfoForPartitions(ctx, partitions)
		if err != nil {
			return nil, fmt.Errorf("get shard info: %w", err)
		}
		res := struct {
			ShardInfo map[string]*model.ShardInfo `json:"shardInfo"`
			Brokers   []*model.Broker             `json:"brokers"`
		}{
			ShardInfo: shardInfo,
			Brokers:   brokers,
		}
		clusterInfoBytes, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("marshal shard info: %w", err)
		}
		return clusterInfoBytes, nil

	} else if cmd.CommandType == command.BrokerCommands.BrokerForID {
		if len(cmd.Args) != 1 {
			return nil, fmt.Errorf("invalid command args")
		}
		brokerID, err := strconv.ParseUint(string(cmd.Args[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid command args: %w", err)
		}
		broker, err := f.brokerService.GetBroker(ctx, brokerID)
		if err != nil {
			if stdErrors.Is(err, errors.ErrBrokerNotFound) {
				res := struct {
					Found bool `json:"found"`
				}{
					Found: false,
				}
				return json.Marshal(res)
			}
			return nil, fmt.Errorf("get broker: %w", err)
		}
		res := struct {
			Found  bool          `json:"found"`
			Broker *model.Broker `json:"broker"`
		}{
			Found:  true,
			Broker: broker,
		}
		return json.Marshal(res)
	}
	return nil, fmt.Errorf("invalid command type: %s", cmd.CommandType)
}

func (f *fsm) Sync() error {
	return nil
}

func (f *fsm) PrepareSnapshot() (any, error) {
	return nil, nil
}

func (f *fsm) SaveSnapshot(_ any, writer io.Writer, i2 <-chan struct{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		select {
		case <-i2:
			cancel()
		case <-done:
			break
		}
	}()
	err := f.topicService.Snapshot(ctx, writer)
	close(done)
	return err
}

func (f *fsm) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		select {
		case <-i:
			cancel()
		case <-done:
			break
		}
	}()
	err := f.topicService.RecoverFromSnapshot(ctx, reader)
	close(done)
	return err
}

func (f *fsm) Close() error {
	return nil
}
