package lookup

import (
	"context"
	"fmt"
	"github.com/sreekar2307/queue/logger"

	pbBrokerCommand "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"google.golang.org/protobuf/proto"
)

type (
	partitionIDForMessageBuilder        struct{}
	partitionIDForMessageEncoderDecoder struct{}
)

var kindPartitionIDForMessage = pbCommandTypes.Kind_KIND_PARTITION_ID_FOR_MESSAGE

func (c partitionIDForMessageBuilder) NewLookup(fsm command.BrokerFSM, log logger.Logger) command.Lookup {
	return partitionIDForMessage{
		fsm: fsm,
		log: log,
	}
}

func (c partitionIDForMessageBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return partitionIDForMessageEncoderDecoder{}
}

func (c partitionIDForMessageBuilder) Kind() pbCommandTypes.Kind {
	return kindPartitionIDForMessage
}

func NewPartitionIDForMessageBuilder() command.LookupBrokerBuilder {
	return partitionIDForMessageBuilder{}
}

type partitionIDForMessage struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c partitionIDForMessageEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommand.PartitionIDForMessageInputs)
	if !ok {
		return nil, fmt.Errorf("expected command.paritionIDForMessageInputs, got %T", arg)
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindPartitionIDForMessage,
		Args: args,
	}
	cmdBytes, err := proto.Marshal(&cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	return cmdBytes, nil
}

func (c partitionIDForMessageEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommand.PartitionIDForMessageOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c partitionIDForMessage) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	var ci pbBrokerCommand.PartitionIDForMessageInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return nil, fmt.Errorf("unmarshal command args: %w", err)
	}
	paritionIDForMessage, err := c.fsm.TopicService().PartitionID(ctx, model.FromProtoBufMessage(ci.Message))
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	co := pbBrokerCommand.PartitionIDForMessageOutputs{
		PartitionId: paritionIDForMessage,
	}
	coBytes, err := proto.Marshal(&co)
	if err != nil {
		return nil, fmt.Errorf("marshal broker: %w", err)
	}
	return coBytes, nil
}
