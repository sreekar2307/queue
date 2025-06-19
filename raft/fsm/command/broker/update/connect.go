package update

import (
	"context"
	stdErrors "errors"
	"fmt"
	"github.com/sreekar2307/queue/logger"
	"reflect"
	"slices"

	"github.com/lni/dragonboat/v4/statemachine"
	pbBrokerCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"google.golang.org/protobuf/proto"
)

type (
	connectBuilder        struct{}
	connectEncoderDecoder struct{}
)

var kindConnect = pbCommandTypes.Kind_KIND_CONNECT

func (c connectBuilder) NewUpdate(fsm command.BrokerFSM, log logger.Logger) command.Update {
	return connect{
		fsm: fsm,
		log: log,
	}
}

func (c connectBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return connectEncoderDecoder{}
}

func (c connectBuilder) Kind() pbCommandTypes.Kind {
	return kindConnect
}

func NewConnectBuilder() command.UpdateBrokerBuilder {
	return connectBuilder{}
}

type connect struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c connectEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommandTypes.ConnectInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.ConnectInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindConnect,
		Args: args,
	}
	return proto.Marshal(&cmd)
}

func (c connectEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommandTypes.ConnectOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c connect) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbBrokerCommandTypes.ConnectInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	consumer, consumerGroup, err := c.fsm.ConsumerService().Connect(
		ctx,
		entry.Index,
		ci.ConsumerGroupId,
		ci.ConsumerId,
		ci.Topics,
	)
	var result pbBrokerCommandTypes.ConnectOutputs
	if err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  nil,
				},
			}, nil
		} else if stdErrors.Is(err, storageErrors.ErrTopicNotFound) {
			result.TopicsNotFound = true
			resultBytes, err := proto.Marshal(&result)
			if err != nil {
				return empty, fmt.Errorf("marshal consumer: %w", err)
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
		return empty, fmt.Errorf("create consumer: %w", err)
	}
	result = pbBrokerCommandTypes.ConnectOutputs{
		ConsumerGroup:  consumerGroup.ToProtoBuf(),
		Consumer:       consumer.ToProtoBuf(),
		TopicsNotFound: false,
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
