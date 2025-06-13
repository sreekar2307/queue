package message

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/sreekar2307/queue/config"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"github.com/sreekar2307/queue/service"
	messageServ "github.com/sreekar2307/queue/service/message"
	"github.com/sreekar2307/queue/storage"
	messageStorage "github.com/sreekar2307/queue/storage/message"
	"io"
	"log"
	"path/filepath"
	"slices"
	"strconv"
)

type fsm struct {
	shardID        uint64
	replicaID      uint64
	messageService service.MessageService
	broker         *model.Broker
}

func NewMessageFSM(
	shardID, replicaID uint64,
	broker *model.Broker,
	mdStorage storage.MetadataStorage,
) statemachine.IOnDiskStateMachine {
	partitionsStorePath := filepath.Join(
		config.Conf.PartitionsPath,
		strconv.Itoa(int(shardID)),
		strconv.Itoa(int(replicaID)),
	)
	return &fsm{
		messageService: messageServ.NewDefaultMessageService(
			messageStorage.NewBolt(
				partitionsStorePath,
			),
			mdStorage,
			partitionsStorePath,
			broker,
		),
		broker:    broker,
		shardID:   shardID,
		replicaID: replicaID,
	}
}

func (f fsm) MessageService() service.MessageService {
	return f.messageService
}

func (f fsm) Broker() *model.Broker {
	return f.broker
}

func (f fsm) Open(_ <-chan struct{}) (uint64, error) {
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

func (f fsm) Update(entries []statemachine.Entry) (results []statemachine.Entry, _ error) {
	ctx := context.Background()
	for _, entry := range entries {
		var cmd command.Cmd
		if err := json.Unmarshal(entry.Cmd, &cmd); err != nil {
			return nil, fmt.Errorf("unmarshing cmd: %w", err)
		}
		log.Println("Processing command", cmd.CommandType, "with args", cmd.Args,
			"at index", entry.Index, "for message fsm")
		if cmd.CommandType == command.MessageCommands.Append {
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
		} else if cmd.CommandType == command.MessageCommands.Ack {
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
			log.Println("acknowledging message", msg.ID, " to consumer group", string(args[0]),
				" of partition ", msg.PartitionID)
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

func (f fsm) Lookup(i any) (any, error) {
	var (
		cmd command.Cmd
		ctx = context.Background()
	)
	if err := json.Unmarshal(i.([]byte), &cmd); err != nil {
		return nil, fmt.Errorf("unmarshing cmd: %w", err)
	}
	if cmd.CommandType == command.MessageCommands.Poll {
		args := cmd.Args
		if len(args) != 2 {
			return nil, fmt.Errorf("invalid command args")
		}
		msg, err := f.messageService.Poll(ctx, string(args[0]), string(args[1]))
		if err != nil {
			if errors.Is(err, messageServ.ErrNoNewMessages) {
				log.Println("no messages for consumer group",
					string(args[0]), " and partition ", string(args[1]))
				return nil, nil
			}
			return nil, fmt.Errorf("get topic: %w", err)
		}
		log.Println("sending message", msg.ID, " to consumer group", string(args[0]), " of partition ", string(args[1]))
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshal message: %w", err)
		}
		return msgBytes, nil
	}
	return nil, fmt.Errorf("invalid command type: %s", cmd.CommandType)
}

func (f fsm) Sync() error {
	return nil
}

func (f fsm) PrepareSnapshot() (any, error) {
	return nil, nil
}

func (f fsm) SaveSnapshot(_ any, writer io.Writer, i2 <-chan struct{}) error {
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

func (f fsm) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
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

func (f fsm) Close() error {
	ctx := context.Background()
	if err := f.messageService.Close(ctx); err != nil {
		return fmt.Errorf("close message service: %w", err)
	}
	return nil
}
