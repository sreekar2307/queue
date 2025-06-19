package update

import (
	"context"
	"fmt"
	"github.com/sreekar2307/queue/logger"
	"reflect"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"

	storageErrors "github.com/sreekar2307/queue/storage/errors"

	stdErrors "errors"

	"github.com/lni/dragonboat/v4/statemachine"
	pbBrokerCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
)

type (
	createTopicBuilder        struct{}
	createTopicEncoderDecoder struct{}
)

var kindCreateTopic = pbCommandTypes.Kind_KIND_CREATE_TOPIC

func (c createTopicBuilder) NewUpdate(fsm command.BrokerFSM, log logger.Logger) command.Update {
	return createTopic{
		fsm: fsm,
		log: log,
	}
}

func (c createTopicBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return createTopicEncoderDecoder{}
}

func (c createTopicBuilder) Kind() pbCommandTypes.Kind {
	return kindCreateTopic
}

func NewCreateTopicBuilder() command.UpdateBrokerBuilder {
	return createTopicBuilder{}
}

type createTopic struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c createTopicEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommandTypes.CreateTopicInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.CreateTopicInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindCreateTopic,
		Args: args,
	}
	return proto.Marshal(&cmd)
}

func (c createTopicEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommandTypes.CreateTopicOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c createTopic) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbBrokerCommandTypes.CreateTopicInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	topic, err := c.fsm.TopicService().CreateTopic(
		ctx,
		entry.Index,
		ci.Topic,
		ci.NumOfPartitions,
		ci.ShardOffset,
	)
	var result pbBrokerCommandTypes.CreateTopicOutputs
	if err != nil {
		if stdErrors.Is(err, storageErrors.ErrTopicAlreadyExists) {
			resultBytes, err := proto.Marshal(&result)
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
		} else if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			resultBytes, err := proto.Marshal(&result)
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
	result = pbBrokerCommandTypes.CreateTopicOutputs{
		Topic: &pbTypes.Topic{
			Topic:           topic.Name,
			NumOfPartitions: topic.NumberOfPartitions,
		},
		IsCreated: true,
	}
	resultBytes, err := proto.Marshal(&result)
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
