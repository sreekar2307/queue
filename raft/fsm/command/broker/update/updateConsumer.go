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
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"google.golang.org/protobuf/proto"
)

type (
	updateConsumerBuilder        struct{}
	updateConsumerEncoderDecoder struct{}
)

var kindupdateConsumer = pbCommandTypes.Kind_KIND_UPDATE_CONSUMER

func (c updateConsumerBuilder) NewUpdate(fsm command.BrokerFSM, log logger.Logger) command.Update {
	return updateConsumer{
		fsm: fsm,
		log: log,
	}
}

func (c updateConsumerBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return updateConsumerEncoderDecoder{}
}

func (c updateConsumerBuilder) Kind() pbCommandTypes.Kind {
	return kindupdateConsumer
}

func NewUpdateConsumerBuilder() command.UpdateBrokerBuilder {
	return updateConsumerBuilder{}
}

type updateConsumer struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c updateConsumerEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommandTypes.UpdateConsumerInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.updateConsumerInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindupdateConsumer,
		Args: args,
	}
	return proto.Marshal(&cmd)
}

func (c updateConsumerEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommandTypes.UpdateConsumerOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c updateConsumer) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbBrokerCommandTypes.UpdateConsumerInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	updatedConsumer, err := c.fsm.ConsumerService().UpdateConsumer(ctx, entry.Index, model.FromProtoBufConsumer(ci.Consumer))
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
		}
		return empty, fmt.Errorf("update consumer: %w", err)
	}
	co := pbBrokerCommandTypes.UpdateConsumerOutputs{
		Consumer: updatedConsumer.ToProtoBuf(),
	}
	coBytes, err := proto.Marshal(&co)
	if err != nil {
		return empty, fmt.Errorf("marshal consumer: %w", err)
	}
	return statemachine.Entry{
		Index: entry.Index,
		Cmd:   slices.Clone(entry.Cmd),
		Result: statemachine.Result{
			Value: entry.Index,
			Data:  coBytes,
		},
	}, nil
}
