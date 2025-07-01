package update

import (
	"context"
	"errors"
	stdErrors "errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/sreekar2307/queue/logger"

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
	appendBuilder        struct{}
	appendEncoderDecoder struct{}
)

var kindappend = pbCommandTypes.Kind_KIND_MESSAGE_APPEND

func (c appendBuilder) NewUpdate(fsm command.MessageFSM, log logger.Logger) command.Update {
	return appendMsg{
		fsm: fsm,
		log: log,
	}
}

func (c appendBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return appendEncoderDecoder{}
}

func (c appendBuilder) Kind() pbCommandTypes.Kind {
	return kindappend
}

func NewAppendBuilder() command.UpdateMessageBuilder {
	return appendBuilder{}
}

type appendMsg struct {
	fsm command.MessageFSM
	log logger.Logger
}

func (c appendEncoderDecoder) EncodeArgs(_ context.Context, arg any, headers map[string]string) ([]byte, error) {
	ca, ok := arg.(*pbMessageCommandTypes.AppendInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.appendInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:     kindappend,
		Args:    args,
		Headers: headers,
	}
	return proto.Marshal(&cmd)
}

func (c appendEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbMessageCommandTypes.AppendOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command results: %w", err)
	}
	return &co, nil
}

func (c appendMsg) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbMessageCommandTypes.AppendInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	msg := model.FromProtoBufMessage(ci.Message)
	err := c.fsm.MessageService().AppendMessage(
		ctx,
		entry.Index,
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
	co := &pbMessageCommandTypes.AppendOutputs{
		Message: msg.ToProtoBuf(),
	}
	resultBytes, err := proto.Marshal(co)
	if err != nil {
		return empty, fmt.Errorf("marshal result: %w", err)
	}
	return statemachine.Entry{
		Index: entry.Index,
		Cmd:   slices.Clone(entry.Cmd),
		Result: statemachine.Result{
			Value: entry.Index,
			Data:  resultBytes,
		},
	}, nil
}
