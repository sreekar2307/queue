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
	healthCheckBuilder        struct{}
	healthCheckEncoderDecoder struct{}
)

var kindhealthCheck = pbCommandTypes.Kind_KIND_HEALTH_CHECK

func (c healthCheckBuilder) NewUpdate(fsm command.BrokerFSM, log logger.Logger) command.Update {
	return healthCheck{
		fsm: fsm,
		log: log,
	}
}

func (c healthCheckBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return healthCheckEncoderDecoder{}
}

func (c healthCheckBuilder) Kind() pbCommandTypes.Kind {
	return kindhealthCheck
}

func NewHealthCheckBuilder() command.UpdateBrokerBuilder {
	return healthCheckBuilder{}
}

type healthCheck struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c healthCheckEncoderDecoder) EncodeArgs(_ context.Context, arg any, headers map[string]string) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommandTypes.HealthCheckInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.healthCheckInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:     kindhealthCheck,
		Args:    args,
		Headers: headers,
	}
	return proto.Marshal(&cmd)
}

func (c healthCheckEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommandTypes.HealthCheckOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c healthCheck) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbBrokerCommandTypes.HealthCheckInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	consumer, err := c.fsm.ConsumerService().HealthCheck(ctx, entry.Index, ci.ConsumerId, ci.PingAt.AsTime().Unix())
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
	co := pbBrokerCommandTypes.HealthCheckOutputs{
		Consumer: consumer.ToProtoBuf(),
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
