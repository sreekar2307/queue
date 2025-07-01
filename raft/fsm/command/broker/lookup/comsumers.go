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
	consumersBuilder        struct{}
	consumersEncoderDecoder struct{}
)

var kindConsumers = pbCommandTypes.Kind_KIND_CONSUMERS

func (c consumersBuilder) NewLookup(fsm command.BrokerFSM, log logger.Logger) command.Lookup {
	return consumers{
		fsm: fsm,
		log: log,
	}
}

func (c consumersBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return consumersEncoderDecoder{}
}

func (c consumersBuilder) Kind() pbCommandTypes.Kind {
	return kindConsumers
}

func NewConsumersBuilder() command.LookupBrokerBuilder {
	return consumersBuilder{}
}

type consumers struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c consumersEncoderDecoder) EncodeArgs(_ context.Context, arg any, headers map[string]string) ([]byte, error) {
	cmd := pbCommandTypes.Cmd{
		Cmd:     kindConsumers,
		Headers: headers,
	}
	cmdBytes, err := proto.Marshal(&cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	return cmdBytes, nil
}

func (c consumersEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommand.ConsumersOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c consumers) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	consumers, err := c.fsm.ConsumerService().AllConsumers(ctx)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	var co pbBrokerCommand.ConsumersOutputs
	for _, consumer := range consumers {
		co.Consumers = append(co.Consumers, consumer.ToProtoBuf())
	}
	coBytes, err := proto.Marshal(&co)
	if err != nil {
		return nil, fmt.Errorf("marshal broker: %w", err)
	}
	return coBytes, nil
}
