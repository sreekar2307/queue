package lookup

import (
	"context"
	"fmt"
	"github.com/sreekar2307/queue/logger"
	"reflect"

	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
	"github.com/sreekar2307/queue/model"
	"google.golang.org/protobuf/proto"

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

var kindTopicForID = pbCommandTypes.Kind_KIND_TOPIC_FOR_ID

func (c topicForIDBuilder) NewLookup(fsm command.BrokerFSM, log logger.Logger) command.Lookup {
	return topicForID{
		fsm: fsm,
		log: log,
	}
}

func (c topicForIDBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return topicForIDEncoderDecoder{}
}

func (c topicForIDBuilder) Kind() pbCommandTypes.Kind {
	return kindTopicForID
}

func NewTopicForIDBuilder() command.LookupBrokerBuilder {
	return topicForIDBuilder{}
}

type topicForID struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c topicForIDEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommand.TopicForIDInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.TopicForIDInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
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
	var pbTopic pbTypes.Topic
	if err := proto.Unmarshal(bytes, &pbTopic); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return model.FromProtoBufTopic(&pbTopic), nil
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
