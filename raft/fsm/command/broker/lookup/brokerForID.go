package lookup

import (
	"context"
	"fmt"
	"github.com/sreekar2307/queue/logger"

	pbBrokerCommand "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"google.golang.org/protobuf/proto"
)

type (
	brokerForIDBuilder        struct{}
	brokerForIDEncoderDecoder struct{}
)

var kindBrokerForID = pbCommandTypes.Kind_KIND_BROKER_FOR_ID

func (c brokerForIDBuilder) NewLookup(fsm command.BrokerFSM, log logger.Logger) command.Lookup {
	return brokerForID{
		fsm: fsm,
		log: log,
	}
}

func (c brokerForIDBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return brokerForIDEncoderDecoder{}
}

func (c brokerForIDBuilder) Kind() pbCommandTypes.Kind {
	return kindBrokerForID
}

func NewBrokerForIDBuilder() command.LookupBrokerBuilder {
	return brokerForIDBuilder{}
}

type brokerForID struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c brokerForIDEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommand.BrokerForIDInputs)
	if !ok {
		return nil, fmt.Errorf("expected command.BrokerForIDInputs, got %T", arg)
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindBrokerForID,
		Args: args,
	}
	cmdBytes, err := proto.Marshal(&cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	return cmdBytes, nil
}

func (c brokerForIDEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommand.BrokerForIDOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c brokerForID) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	var ci pbBrokerCommand.BrokerForIDInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return nil, fmt.Errorf("unmarshal command inputs: %w", err)
	}
	broker, err := c.fsm.BrokerService().GetBroker(ctx, ci.BrokerId)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	co := pbBrokerCommand.BrokerForIDOutputs{
		Broker: broker.ToProtoBuf(),
	}
	coBytes, err := proto.Marshal(&co)
	if err != nil {
		return nil, fmt.Errorf("marshal broker: %w", err)
	}
	return coBytes, nil
}
