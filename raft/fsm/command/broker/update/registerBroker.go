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
	registerBrokerBuilder        struct{}
	registerBrokerEncoderDecoder struct{}
)

var kindregisterBroker = pbCommandTypes.Kind_KIND_REGISTER_BROKER

func (c registerBrokerBuilder) NewUpdate(fsm command.BrokerFSM, log logger.Logger) command.Update {
	return registerBroker{
		fsm: fsm,
		log: log,
	}
}

func (c registerBrokerBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return registerBrokerEncoderDecoder{}
}

func (c registerBrokerBuilder) Kind() pbCommandTypes.Kind {
	return kindregisterBroker
}

func NewRegisterBrokerBuilder() command.UpdateBrokerBuilder {
	return registerBrokerBuilder{}
}

type registerBroker struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c registerBrokerEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommandTypes.RegisterBrokerInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.registerBrokerInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindregisterBroker,
		Args: args,
	}
	return proto.Marshal(&cmd)
}

func (c registerBrokerEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	return nil, nil
}

func (c registerBroker) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbBrokerCommandTypes.RegisterBrokerInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	_, err := c.fsm.BrokerService().RegisterBroker(ctx, entry.Index, model.FromProtoBufBroker(ci.Broker))
	if err != nil {
		if !stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return empty, fmt.Errorf("register broker: %w", err)
		}
	}
	return statemachine.Entry{
		Index: entry.Index,
		Cmd:   slices.Clone(entry.Cmd),
		Result: statemachine.Result{
			Value: entry.Index,
			Data:  nil,
		},
	}, nil
}
