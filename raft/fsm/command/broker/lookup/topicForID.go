package lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"

	stdErrors "errors"
)

type (
	topicForIDBuilder        struct{}
	topicForIDEncoderDecoder struct{}
)

var kind = command.TopicCommands.TopicForID

func (c topicForIDBuilder) NewLookup(fsm command.BrokerFSM) command.Lookup {
	return topicForID{
		fsm: fsm,
	}
}

func (c topicForIDBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return topicForIDEncoderDecoder{}
}

func (c topicForIDBuilder) Kind() command.Kind {
	return kind
}

func NewTopicForIDBuilder() command.Builder {
	return topicForIDBuilder{}
}

type topicForID struct {
	fsm command.BrokerFSM
}

func (c topicForIDEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(command.TopicForIDInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.TopicForIDInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := json.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := command.CmdV2{
		CommandType: kind,
		Args:        args,
	}
	return json.Marshal(cmd)
}

func (c topicForIDEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var topic model.Topic
	if err := json.Unmarshal(bytes, &topic); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return topic, nil
}

func (c topicForID) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	var ci command.TopicForIDInputs
	if err := json.Unmarshal(inputs, &ci); err != nil {
		return nil, fmt.Errorf("unmarshal command args: %w", err)
	}
	topic, err := c.fsm.TopicService().GetTopic(ctx, ci.TopicName)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	topicBytes, err := json.Marshal(topic)
	if err != nil {
		return nil, fmt.Errorf("marshal topic: %w", err)
	}
	return topicBytes, nil
}
