package lookup

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
	"github.com/sreekar2307/queue/model"
	"reflect"

	pbBrokerCommand "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"

	stdErrors "errors"
)

type (
	topicForIDBuilder        struct{}
	topicForIDEncoderDecoder struct{}
)

var kind = pbCommandTypes.Kind_KIND_TOPIC_FOR_ID

func (c topicForIDBuilder) NewLookup(fsm command.BrokerFSM) command.Lookup {
	return topicForID{
		fsm: fsm,
	}
}

func (c topicForIDBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return topicForIDEncoderDecoder{}
}

func (c topicForIDBuilder) Kind() pbCommandTypes.Kind {
	return kind
}

func NewTopicForIDBuilder() command.LookupBrokerBuilder {
	return topicForIDBuilder{}
}

type topicForID struct {
	fsm command.BrokerFSM
}

func (c topicForIDEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(pbBrokerCommand.TopicForIDInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.TopicForIDInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(&ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  pbCommandTypes.Kind_KIND_CREATE_TOPIC,
		Args: args,
	}
	return proto.Marshal(&cmd)
}

func (c topicForIDEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var topic pbTypes.Topic
	if err := proto.Unmarshal(bytes, &topic); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return model.Topic{
		Name:               topic.Topic,
		NumberOfPartitions: topic.NumOfPartitions,
	}, nil
}

func (c topicForID) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	var ci pbBrokerCommand.TopicForIDInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return nil, fmt.Errorf("unmarshal command args: %w", err)
	}
	topic, err := c.fsm.TopicService().GetTopic(ctx, ci.Topic)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	topicBytes, err := proto.Marshal(&pbTypes.Topic{
		Topic:           topic.Name,
		NumOfPartitions: topic.NumberOfPartitions,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal topic: %w", err)
	}
	return topicBytes, nil
}
