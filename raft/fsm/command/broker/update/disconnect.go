package update

import (
	"context"
	stdErrors "errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/sreekar2307/queue/logger"

	"github.com/lni/dragonboat/v4/statemachine"
	pbBrokerCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"google.golang.org/protobuf/proto"
)

type (
	DisconnectBuilder        struct{}
	DisconnectEncoderDecoder struct{}
)

var kindDisconnect = pbCommandTypes.Kind_KIND_DISCONNECT

func (c DisconnectBuilder) NewUpdate(fsm command.BrokerFSM, log logger.Logger) command.Update {
	return Disconnect{
		fsm: fsm,
		log: log,
	}
}

func (c DisconnectBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return DisconnectEncoderDecoder{}
}

func (c DisconnectBuilder) Kind() pbCommandTypes.Kind {
	return kindDisconnect
}

func NewDisconnectBuilder() command.UpdateBrokerBuilder {
	return DisconnectBuilder{}
}

type Disconnect struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c DisconnectEncoderDecoder) EncodeArgs(_ context.Context, arg any, headers map[string]string) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommandTypes.DisconnectInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.DisconnectInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:     kindDisconnect,
		Args:    args,
		Headers: headers,
	}
	return proto.Marshal(&cmd)
}

func (c DisconnectEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	return nil, nil
}

func (c Disconnect) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbBrokerCommandTypes.DisconnectInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	err := c.fsm.ConsumerService().Disconnect(ctx, entry.Index, ci.ConsumerId)
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
		return empty, fmt.Errorf("create consumer: %w", err)
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
