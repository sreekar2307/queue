package lookup

import (
	"context"
	"fmt"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
	"github.com/sreekar2307/queue/util"

	pbBrokerCommand "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"google.golang.org/protobuf/proto"
)

type (
	partitionsForTopicBuilder        struct{}
	partitionsForTopicEncoderDecoder struct{}
)

var kindPartitionsForTopic = pbCommandTypes.Kind_KIND_PARTITIONS_FOR_TOPIC

func (c partitionsForTopicBuilder) NewLookup(fsm command.BrokerFSM) command.Lookup {
	return partitionsForTopic{
		fsm: fsm,
	}
}

func (c partitionsForTopicBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return partitionsForTopicEncoderDecoder{}
}

func (c partitionsForTopicBuilder) Kind() pbCommandTypes.Kind {
	return kindPartitionsForTopic
}

func NewPartitionsForTopicBuilder() command.LookupBrokerBuilder {
	return partitionsForTopicBuilder{}
}

type partitionsForTopic struct {
	fsm command.BrokerFSM
}

func (c partitionsForTopicEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommand.PartitionsForTopicInputs)
	if !ok {
		return nil, fmt.Errorf("expected command.partitionsForTopicInputs, got %T", arg)
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindPartitionsForTopic,
		Args: args,
	}
	cmdBytes, err := proto.Marshal(&cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	return cmdBytes, nil
}

func (c partitionsForTopicEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommand.PartitionsForTopicOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c partitionsForTopic) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	var ci pbBrokerCommand.PartitionsForTopicInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return nil, fmt.Errorf("unmarshal command args: %w", err)
	}
	partitions, err := c.fsm.TopicService().GetPartitions(ctx, ci.Topic)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	co := pbBrokerCommand.PartitionsForTopicOutputs{
		Partitions: util.Map(partitions, func(p *model.Partition) *pbTypes.Partition {
			return p.ToProtoBuf()
		}),
	}
	coBytes, err := proto.Marshal(&co)
	if err != nil {
		return nil, fmt.Errorf("marshal broker: %w", err)
	}
	return coBytes, nil
}
