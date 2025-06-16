package update

import (
	"context"
	stdErrors "errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/lni/dragonboat/v4/statemachine"
	pbBrokerCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command"
	cmdErrors "github.com/sreekar2307/queue/raft/fsm/command/errors"
	storageErrors "github.com/sreekar2307/queue/storage/errors"
	"github.com/sreekar2307/queue/util"
	"google.golang.org/protobuf/proto"
)

type (
	partitionAddedBuilder        struct{}
	partitionAddedEncoderDecoder struct{}
)

var kindPartitionAdded = pbCommandTypes.Kind_KIND_PARTITION_ADDED

func (c partitionAddedBuilder) NewUpdate(fsm command.BrokerFSM) command.Update {
	return partitionAdded{
		fsm: fsm,
	}
}

func (c partitionAddedBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return partitionAddedEncoderDecoder{}
}

func (c partitionAddedBuilder) Kind() pbCommandTypes.Kind {
	return kindPartitionAdded
}

func NewPartitionAddedBuilder() command.UpdateBrokerBuilder {
	return partitionAddedBuilder{}
}

type partitionAdded struct {
	fsm command.BrokerFSM
}

func (c partitionAddedEncoderDecoder) EncodeArgs(_ context.Context, arg any) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommandTypes.PartitionAdddedInputs)
	if !ok {
		return nil, stdErrors.Join(cmdErrors.ErrInvalidCommandArgs,
			fmt.Errorf("expected command.PartitionAddedInputs, got %s", reflect.TypeOf(arg)))
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:  kindPartitionAdded,
		Args: args,
	}
	return proto.Marshal(&cmd)
}

func (c partitionAddedEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	return nil, nil
}

func (c partitionAdded) ExecuteUpdate(
	ctx context.Context,
	inputs []byte,
	entry statemachine.Entry,
) (empty statemachine.Entry, _ error) {
	var ci pbBrokerCommandTypes.PartitionAdddedInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return empty, fmt.Errorf("unmarshal command args: %w", err)
	}
	partitionUpdates := &model.Partition{
		Members: ci.Members,
		ShardID: ci.ShardId,
	}
	if err := c.fsm.TopicService().UpdatePartition(ctx, entry.Index, ci.PartitionId, partitionUpdates); err != nil {
		if stdErrors.Is(err, storageErrors.ErrDuplicateCommand) {
			return statemachine.Entry{
				Index: entry.Index,
				Cmd:   slices.Clone(entry.Cmd),
				Result: statemachine.Result{
					Value: entry.Index,
					Data:  nil,
				},
			}, nil
		}
		return empty, fmt.Errorf("update partition: %w", err)
	}
	_, ok := util.FirstMatch(util.Keys(ci.Members), func(k uint64) bool {
		return k == c.fsm.Broker().ID
	})
	if !ok {
		return statemachine.Entry{
			Index: entry.Index,
			Cmd:   slices.Clone(entry.Cmd),
			Result: statemachine.Result{
				Value: entry.Index,
				Data:  nil,
			},
		}, nil
	}
	c.fsm.Broker().JoinShard(partitionUpdates)
	return statemachine.Entry{
		Index: entry.Index,
		Cmd:   slices.Clone(entry.Cmd),
		Result: statemachine.Result{
			Value: entry.Index,
			Data:  nil,
		},
	}, nil
}
