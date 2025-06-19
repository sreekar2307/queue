package message

import (
	"context"
	"fmt"
	"github.com/lni/dragonboat/v4/statemachine"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/logger"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command/factory"
	"github.com/sreekar2307/queue/service"
	"google.golang.org/protobuf/proto"
	"io"
)

type FSM struct {
	shardID        uint64
	replicaID      uint64
	messageService service.MessageService
	broker         *model.Broker
	log            logger.Logger
}

func (f *FSM) Log() logger.Logger {
	return f.log
}

func (f *FSM) SetLog(log logger.Logger) {
	f.log = log
}

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

func (f *FSM) SetMessageService(messageService service.MessageService) {
	f.messageService = messageService
}

func (f *FSM) SetBroker(broker *model.Broker) {
	f.broker = broker
}

func (f *FSM) MessageService() service.MessageService {
	return f.messageService
}

func (f *FSM) Broker() *model.Broker {
	return f.broker
}

func (f *FSM) Open(_ <-chan struct{}) (uint64, error) {
	ctx := context.Background()
	if err := f.messageService.Open(ctx); err != nil {
		return 0, fmt.Errorf("open message service: %w", err)
	}
	commandID, err := f.messageService.LastAppliedCommandID(ctx, f.shardID)
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
		f.log.Debug(
			ctx,
			"Processing command for broker fsm",
			logger.NewAttr("cmd", cmd.Cmd),
			logger.NewAttr("args", cmd.Args),
			logger.NewAttr("index", entry.Index),
		)
		updator, err := factory.MessageExecuteUpdate(cmd.Cmd, f, f.log)
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
	lookup, err := factory.MessageLookup(cmd.Cmd, f, f.log)
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
	err := f.messageService.Snapshot(ctx, writer)
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
	err := f.messageService.RecoverFromSnapshot(ctx, reader)
	close(done)
	return err
}

func (f *FSM) Close() error {
	ctx := context.Background()
	if err := f.messageService.Close(ctx); err != nil {
		return fmt.Errorf("close message service: %w", err)
	}
	return nil
}
