package service

import (
	"context"
	"encoding/json"
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
	mdStorage      storage.MetadataStorage
}

func NewMessageFSM(partitionsPath string, mdStorage storage.MetadataStorage) statemachine.CreateOnDiskStateMachineFunc {
	return func(shardID uint64, replicaID uint64) statemachine.IOnDiskStateMachine {
		partitionsStorePath := filepath.Join(
			partitionsPath,
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
			mdStorage: mdStorage,
			ShardID:   shardID,
			ReplicaID: replicaID,
		}
	}
}

func (f MessageFSM) Open(stopc <-chan struct{}) (uint64, error) {
	ctx := context.Background()
	return 0, f.messageService.Open(ctx)
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
			err := f.messageService.AppendMessage(ctx, &msg)
			if err != nil {
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
					Value: 1,
					Data:  msgBytes,
				},
			})
		}
	}
	return results, nil
}

func (f MessageFSM) Lookup(i any) (any, error) {
	return nil, nil
}

func (f MessageFSM) Sync() error {
	return nil
}

func (f MessageFSM) PrepareSnapshot() (any, error) {
	// TODO implement me
	panic("implement me")
}

func (f MessageFSM) SaveSnapshot(i any, writer io.Writer, i2 <-chan struct{}) error {
	// TODO implement me
	panic("implement me")
}

func (f MessageFSM) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	// TODO implement me
	panic("implement me")
}

func (f MessageFSM) Close() error {
	ctx := context.Background()
	if err := f.messageService.Close(ctx); err != nil {
		return fmt.Errorf("close message service: %w", err)
	}
	return nil
}
