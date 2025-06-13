package update

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"

	"github.com/sreekar2307/queue/storage/errors"

	stdErrors "errors"

	"github.com/lni/dragonboat/v4/statemachine"
)

type (
	createTopicBuilder        struct{}
	createTopicEncoderDecoder struct{}
)

var kind = command.TopicCommands.CreateTopic

func (c createTopicBuilder) NewUpdate(fsm command.BrokerFSM) command.Update {
	return createTopic{
		fsm: fsm,
	}
}

func (c createTopicBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return createTopicEncoderDecoder{}
}

func (c createTopicBuilder) Kind() command.Kind {
	return kind
}

func NewCreateTopicBuilder() command.Builder {
	return createTopicBuilder{}
}

type createTopic struct {
	fsm command.BrokerFSM
}

func (c createTopicEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(command.CreateTopicInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.CreateTopicInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := json.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := command.CmdV2{
		CommandType: kind,
		Args:        args,
	}
	return json.Marshal(cmd)
}

func (c createTopicEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co command.CreateTopicOutputs
	if err := json.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return co, nil
}

func (c createTopic) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci command.CreateTopicInputs
	if err := json.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	topic, err := c.fsm.TopicService().CreateTopic(
		ctx,
		entry.Index,
		ci.TopicName,
		ci.NumberOfPartitions,
		ci.ShardOffset,
	)
	var result command.CreateTopicOutputs
	if err != nil {
		if stdErrors.Is(err, errors.ErrTopicAlreadyExists) {
			resultBytes, err := json.Marshal(result)
			if err != nil {
				return empty, fmt.Errorf("marshal result: %w", err)
			}
			return statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  resultBytes,
				},
			}, nil
		} else if stdErrors.Is(err, errors.ErrDuplicateCommand) {
			resultBytes, err := json.Marshal(result)
			if err != nil {
				return empty, fmt.Errorf("marshal result: %w", err)
			}
			return statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  resultBytes,
				},
			}, nil
		}
		return empty, fmt.Errorf("create topic: %w", err)
	}
	result = command.CreateTopicOutputs{
		Topic:   topic,
		Created: true,
	}
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return empty, fmt.Errorf("marshal result: %w", err)
	}
	return statemachine.Entry{
		Index: entry.Index,
		Cmd:   slices.Clone(entry.Cmd),
		Result: statemachine.Result{
			Value: entry.Index,
			Data:  resultBytes,
		},
	}, nil
}
