package broker

import (
	"context"
	"fmt"
	"io"
	"log"

	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"google.golang.org/protobuf/proto"

	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command/factory"
	"github.com/sreekar2307/queue/service"
	"github.com/sreekar2307/queue/storage"

	"github.com/lni/dragonboat/v4/statemachine"
)

type (
	FSM struct {
		shardID         uint64
		replicaID       uint64
		mdStorage       storage.MetadataStorage
		topicService    service.TopicService
		consumerService service.ConsumerService
		brokerService   service.BrokerService
		broker          *model.Broker
	}
)

func (f *FSM) ShardID() uint64 {
	return f.shardID
}

func (f *FSM) SetShardID(shardID uint64) {
	f.shardID = shardID
}

func (f *FSM) ReplicaID() uint64 {
	return f.replicaID
}

func (f *FSM) SetReplicaID(replicaID uint64) {
	f.replicaID = replicaID
}

func (f *FSM) SetMdStorage(mdStorage storage.MetadataStorage) {
	f.mdStorage = mdStorage
}

func (f *FSM) SetTopicService(topicService service.TopicService) {
	f.topicService = topicService
}

func (f *FSM) SetConsumerService(consumerService service.ConsumerService) {
	f.consumerService = consumerService
}

func (f *FSM) SetBrokerService(brokerService service.BrokerService) {
	f.brokerService = brokerService
}

func (f *FSM) SetBroker(broker *model.Broker) {
	f.broker = broker
}

func (f *FSM) TopicService() service.TopicService {
	return f.topicService
}

func (f *FSM) ConsumerService() service.ConsumerService {
	return f.consumerService
}

func (f *FSM) BrokerService() service.BrokerService {
	return f.brokerService
}

func (f *FSM) Broker() *model.Broker {
	return f.broker
}

func (f *FSM) MdStorage() storage.MetadataStorage {
	return f.mdStorage
}

func (f *FSM) Open(_ <-chan struct{}) (uint64, error) {
	ctx := context.Background()
	commandID, err := f.topicService.LastAppliedCommandID(ctx, f.shardID)
	if err != nil {
		return 0, fmt.Errorf("get last applied command ID: %w", err)
	}
	return commandID, nil
}

func (f *FSM) Update(entries []statemachine.Entry) (results []statemachine.Entry, _ error) {
	ctx := context.Background()
	for _, entry := range entries {
		var cmd pbCommandTypes.Cmd
		if err := proto.Unmarshal(entry.Cmd, &cmd); err != nil {
			return nil, fmt.Errorf("unmarshing cmd: %w", err)
		}
		log.Println("Processing command", cmd.Cmd, "with args", cmd.Args,
			"at index", entry.Index, "for broker fsm")
		updator, err := factory.BrokerExecuteUpdate(cmd.Cmd, f)
		if err != nil {
			return nil, fmt.Errorf("get update for command %s: %w", cmd.Cmd, err)
		}
		resEntry, err := updator.ExecuteUpdate(ctx, cmd.Args, entry)
		if err != nil {
			return nil, fmt.Errorf("execute update for command %s: %w", cmd.Cmd, err)
		}
		results = append(results, resEntry)
	}
	return results, nil
}

func (f *FSM) Lookup(i any) (any, error) {
	var (
		cmd pbCommandTypes.Cmd
		ctx = context.Background()
	)
	if err := proto.Unmarshal(i.([]byte), &cmd); err != nil {
		return nil, fmt.Errorf("unmarshing cmd: %w", err)
	}
	lookup, err := factory.BrokerLookup(cmd.Cmd, f)
	if err != nil {
		return nil, fmt.Errorf("get lookup for command %s: %w", cmd.Cmd, err)
	}
	resEntry, err := lookup.Lookup(ctx, cmd.Args)
	if err != nil {
		return nil, fmt.Errorf("execute lookup for command %s: %w", cmd.Cmd, err)
	}
	return resEntry, nil
}

func (f *FSM) Sync() error {
	return nil
}

func (f *FSM) PrepareSnapshot() (any, error) {
	return nil, nil
}

func (f *FSM) SaveSnapshot(_ any, writer io.Writer, i2 <-chan struct{}) error {
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

func (f *FSM) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
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

func (f *FSM) Close() error {
	return nil
}
