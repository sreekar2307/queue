package update

import (
	"context"
	"errors"
	stdErrors "errors"
	"fmt"
	"log"
	"reflect"
	"slices"

	"github.com/lni/dragonboat/v4/statemachine"
	pbMessageCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/message/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"google.golang.org/protobuf/proto"
)

type (
	ackBuilder        struct{}
	ackEncoderDecoder struct{}
)

var kindack = pbCommandTypes.Kind_KIND_MESSAGE_ACK

func (c ackBuilder) NewUpdate(fsm command.MessageFSM) command.Update {
	return ack{
		fsm: fsm,
	}
}

func (c ackBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return ackEncoderDecoder{}
}

func (c ackBuilder) Kind() pbCommandTypes.Kind {
	return kindack
}

func NewAckBuilder() command.UpdateMessageBuilder {
	return ackBuilder{}
}

type ack struct {
	fsm command.MessageFSM
}

func (c ackEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbMessageCommandTypes.AckInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.ackInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindack,
		Args: args,
	}
	return proto.Marshal(&cmd)
}

func (c ackEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	return nil, nil
}

func (c ack) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbMessageCommandTypes.AckInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	msg := model.FromProtoBufMessage(ci.Message)
	err := c.fsm.MessageService().AckMessage(
		ctx,
		entry.Index,
		ci.ConsumerGroupId,
		msg,
	)
	if err != nil {
		if errors.Is(err, storageErrors.ErrDuplicateCommand) {
			return statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
				},
			}, nil
		}
		return empty, fmt.Errorf("append msg: %w", err)
	}
	log.Println(
		"acknowledging message",
		msg.ID,
		" to consumer group",
		ci.ConsumerGroupId,
		" of partition ",
		msg.PartitionID,
	)
	return statemachine.Entry{
		Index: entry.Index,
		Cmd:   slices.Clone(entry.Cmd),
		Result: statemachine.Result{
			Value: entry.Index,
		},
	}, nil
}
