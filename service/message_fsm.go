package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"queue/model"
	messageServ "queue/service/message"
	"queue/storage"
	messageStorage "queue/storage/message"
	"slices"
	"strconv"

	"github.com/lni/dragonboat/v4/statemachine"
)

type MessageFSM struct {
	ShardID        uint64
	ReplicaID      uint64
	messageService MessageService
	broker         *model.Broker
	config         Config
}

func NewMessageFSM(
	shardID, replicaID uint64,
	config Config,
	broker *model.Broker,
	mdStorage storage.MetadataStorage,
) statemachine.IOnDiskStateMachine {
	partitionsStorePath := filepath.Join(
		config.PartitionsPath,
		strconv.Itoa(int(shardID)),
		strconv.Itoa(int(replicaID)),
	)
	return &MessageFSM{
		messageService: messageServ.NewDefaultMessageService(
			messageStorage.NewBolt(
				partitionsStorePath,
			),
			mdStorage,
			partitionsStorePath,
		),
		broker:    broker,
		config:    config,
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
}

func (f MessageFSM) Open(_ <-chan struct{}) (uint64, error) {
	ctx := context.Background()
	if err := f.messageService.Open(ctx); err != nil {
		return 0, fmt.Errorf("open message service: %w", err)
	}
	commandID, err := f.messageService.LastAppliedCommandID(ctx)
	if err != nil {
		return 0, fmt.Errorf("get last applied command ID: %w", err)
	}
	return commandID, nil
}

func (f MessageFSM) Update(entries []statemachine.Entry) (results []statemachine.Entry, _ error) {
	ctx := context.Background()
	for _, entry := range entries {
		var cmd Cmd
		if err := json.Unmarshal(entry.Cmd, &cmd); err != nil {
			return nil, fmt.Errorf("unmarshing cmd: %w", err)
		}
		if cmd.CommandType == MessageCommands.Append {
			args := cmd.Args
			if len(args) != 1 {
				return nil, fmt.Errorf("invalid command args")
			}
			var msg model.Message
			if err := json.Unmarshal(args[0], &msg); err != nil {
				return nil, fmt.Errorf("unmarshing message: %w", err)
			}
			err := f.messageService.AppendMessage(ctx, entry.Index, &msg)
			if err != nil {
				if errors.Is(err, messageServ.ErrDuplicateCommand) {
					results = append(results, statemachine.Entry{
						Index: entry.Index,
						Cmd:   slices.Clone(entry.Cmd),
						Result: statemachine.Result{
							Value: entry.Index,
						},
					})
					continue
				}
				return nil, fmt.Errorf("append msg: %w", err)
			}

			msgBytes, err := json.Marshal(msg)
			if err != nil {
				return nil, fmt.Errorf("marshal msg: %w", err)
			}
			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  msgBytes,
				},
			})
		} else if cmd.CommandType == MessageCommands.Ack {
			args := cmd.Args
			if len(args) != 2 {
				return nil, fmt.Errorf("invalid command args")
			}
			var msg model.Message
			if err := json.Unmarshal(args[1], &msg); err != nil {
				return nil, fmt.Errorf("unmarshing message: %w", err)
			}
			err := f.messageService.AckMessage(ctx, entry.Index, string(args[0]), &msg)
			if err != nil {
				if errors.Is(err, messageServ.ErrDuplicateCommand) {
					results = append(results, statemachine.Entry{
						Index: entry.Index,
						Cmd:   slices.Clone(entry.Cmd),
						Result: statemachine.Result{
							Value: entry.Index,
						},
					})
					continue
				}
				return nil, fmt.Errorf("append msg: %w", err)
			}
			results = append(results, statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
				},
			})
		} else {
			return nil, fmt.Errorf("invalid command type: %s", cmd.CommandType)
		}
	}
	return results, nil
}

func (f MessageFSM) Lookup(i any) (any, error) {
	var (
		cmd Cmd
		ctx = context.Background()
	)
	if err := json.Unmarshal(i.([]byte), &cmd); err != nil {
		return nil, fmt.Errorf("unmarshing cmd: %w", err)
	}
	if cmd.CommandType == MessageCommands.Poll {
		args := cmd.Args
		if len(args) != 2 {
			return nil, fmt.Errorf("invalid command args")
		}
		msg, err := f.messageService.Poll(ctx, string(args[0]), string(args[1]))
		if err != nil {
			if errors.Is(err, messageServ.ErrNoNewMessages) {
				return nil, nil
			}
			return nil, fmt.Errorf("get topic: %w", err)
		}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshal message: %w", err)
		}
		return msgBytes, nil
	}
	return nil, fmt.Errorf("invalid command type: %s", cmd.CommandType)
}

func (f MessageFSM) Sync() error {
	return nil
}

func (f MessageFSM) PrepareSnapshot() (any, error) {
	return nil, nil
}

func (f MessageFSM) SaveSnapshot(_ any, writer io.Writer, i2 <-chan struct{}) error {
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

func (f MessageFSM) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
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

func (f MessageFSM) Close() error {
	ctx := context.Background()
	if err := f.messageService.Close(ctx); err != nil {
		return fmt.Errorf("close message service: %w", err)
	}
	return nil
}
