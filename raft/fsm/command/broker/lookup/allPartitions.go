package lookup

import (
	"context"
	"fmt"

	pbBrokerCommand "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/logger"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"google.golang.org/protobuf/proto"
)

type (
	allPartitionsBuilder        struct{}
	allPartitionsEncoderDecoder struct{}
)

var kindAllPartitions = pbCommandTypes.Kind_KIND_ALL_PARTITIONS

func (c allPartitionsBuilder) NewLookup(fsm command.BrokerFSM, log logger.Logger) command.Lookup {
	return allPartitions{
		fsm: fsm,
		log: log,
	}
}

func (c allPartitionsBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return allPartitionsEncoderDecoder{}
}

func (c allPartitionsBuilder) Kind() pbCommandTypes.Kind {
	return kindAllPartitions
}

func NewAllPartitionsBuilder() command.LookupBrokerBuilder {
	return allPartitionsBuilder{}
}

type allPartitions struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c allPartitionsEncoderDecoder) EncodeArgs(_ context.Context, arg any, headers map[string]string) ([]byte, error) {
	cmd := pbCommandTypes.Cmd{
		Cmd:     kindAllPartitions,
		Headers: headers,
	}
	return proto.Marshal(&cmd)
}

func (c allPartitionsEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommand.AllPartitionsOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c allPartitions) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	allPartitions, err := c.fsm.TopicService().AllPartitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	var co pbBrokerCommand.AllPartitionsOutputs
	for _, partition := range allPartitions {
		co.Partitions = append(co.Partitions, partition.ToProtoBuf())
	}
	topicBytes, err := proto.Marshal(&co)
	if err != nil {
		return nil, fmt.Errorf("marshal topic: %w", err)
	}
	return topicBytes, nil
}
