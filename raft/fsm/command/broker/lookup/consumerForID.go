package lookup

import (
	"context"
	"fmt"

	pbBrokerCommand "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"google.golang.org/protobuf/proto"
)

type (
	consumerForIDBuilder        struct{}
	consumerForIDEncoderDecoder struct{}
)

var kindconsumerForID = pbCommandTypes.Kind_KIND_CONSUMER_FOR_ID

func (c consumerForIDBuilder) NewLookup(fsm command.BrokerFSM) command.Lookup {
	return consumerForID{
		fsm: fsm,
	}
}

func (c consumerForIDBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return consumerForIDEncoderDecoder{}
}

func (c consumerForIDBuilder) Kind() pbCommandTypes.Kind {
	return kindconsumerForID
}

func NewConsumerForIDBuilder() command.LookupBrokerBuilder {
	return consumerForIDBuilder{}
}

type consumerForID struct {
	fsm command.BrokerFSM
}

func (c consumerForIDEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommand.ConsumerForIDInputs)
	if !ok {
		return nil, fmt.Errorf("expected command.ConsumerForIDInputs, got %T", arg)
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindconsumerForID,
		Args: args,
	}
	cmdBytes, err := proto.Marshal(&cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	return cmdBytes, nil
}

func (c consumerForIDEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommand.ConsumerForIDOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c consumerForID) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	var ci pbBrokerCommand.ConsumerForIDInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return nil, fmt.Errorf("unmarshal command args: %w", err)
	}
	consumerForID, err := c.fsm.ConsumerService().GetConsumer(ctx, ci.ConsumerId)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	co := pbBrokerCommand.ConsumerForIDOutputs{
		Consumer: consumerForID.ToProtoBuf(),
	}
	coBytes, err := proto.Marshal(&co)
	if err != nil {
		return nil, fmt.Errorf("marshal broker: %w", err)
	}
	return coBytes, nil
}
