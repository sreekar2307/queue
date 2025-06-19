package update

import (
	"context"
	"fmt"
	"github.com/sreekar2307/queue/logger"

	stdErrors "errors"
	pbMessageCommand "github.com/sreekar2307/queue/gen/raft/fsm/message/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/raft/fsm/command"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"google.golang.org/protobuf/proto"
)

type (
	pollBuilder        struct{}
	pollEncoderDecoder struct{}
)

var kindPoll = pbCommandTypes.Kind_KIND_MESSAGE_POLL

func (c pollBuilder) NewLookup(fsm command.MessageFSM, log logger.Logger) command.Lookup {
	return poll{
		fsm: fsm,
		log: log,
	}
}

func (c pollBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return pollEncoderDecoder{}
}

func (c pollBuilder) Kind() pbCommandTypes.Kind {
	return kindPoll
}

func NewPollBuilder() command.LookupMessageBuilder {
	return pollBuilder{}
}

type poll struct {
	fsm command.MessageFSM
	log logger.Logger
}

func (c pollEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbMessageCommand.PollInputs)
	if !ok {
		return nil, fmt.Errorf("expected command.PollInputs, got %T", arg)
	}
	caBytes, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindPoll,
		Args: caBytes,
	}
	return proto.Marshal(&cmd)
}

func (c pollEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbMessageCommand.PollOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c poll) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	var ca pbMessageCommand.PollInputs
	if err := proto.Unmarshal(inputs, &ca); err != nil {
		return nil, fmt.Errorf("unmarshal command inputs: %w", err)
	}
	msg, err := c.fsm.MessageService().Poll(ctx, ca.ConsumerGroupId, ca.PartitionId)
	if err != nil {
		if !stdErrors.Is(err, storageErrors.ErrNoMessageFound) {
			return nil, fmt.Errorf("get topic: %w", err)
		}
	}
	co := new(pbMessageCommand.PollOutputs)
	if msg != nil {
		co.Message = msg.ToProtoBuf()
	}
	return proto.Marshal(co)
}
