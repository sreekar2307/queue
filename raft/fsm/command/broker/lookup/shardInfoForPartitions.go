package lookup

import (
	"context"
	"fmt"

	"github.com/sreekar2307/queue/logger"

	pbBrokerCommand "github.com/sreekar2307/queue/gen/raft/fsm/broker/v1"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/raft/fsm/command"
	"google.golang.org/protobuf/proto"
)

type (
	shardInfoForPartitionsBuilder        struct{}
	shardInfoForPartitionsEncoderDecoder struct{}
)

var kindShardInfoForPartitions = pbCommandTypes.Kind_KIND_SHARD_INFO_FOR_PARTITIONS

func (c shardInfoForPartitionsBuilder) NewLookup(fsm command.BrokerFSM, log logger.Logger) command.Lookup {
	return shardInfoForPartitions{
		fsm: fsm,
		log: log,
	}
}

func (c shardInfoForPartitionsBuilder) NewEncoderDecoder() command.EncoderDecoder {
	return shardInfoForPartitionsEncoderDecoder{}
}

func (c shardInfoForPartitionsBuilder) Kind() pbCommandTypes.Kind {
	return kindShardInfoForPartitions
}

func NewShardInfoForPartitionsBuilder() command.LookupBrokerBuilder {
	return shardInfoForPartitionsBuilder{}
}

type shardInfoForPartitions struct {
	fsm command.BrokerFSM
	log logger.Logger
}

func (c shardInfoForPartitionsEncoderDecoder) EncodeArgs(_ context.Context, arg any, headers map[string]string) ([]byte, error) {
	ca, ok := arg.(*pbBrokerCommand.ShardInfoForPartitionsInputs)
	if !ok {
		return nil, fmt.Errorf("expected command.shardInfoForPartitionsInputs, got %T", arg)
	}
	args, err := proto.Marshal(ca)
	if err != nil {
		return nil, fmt.Errorf("marshal command args: %w", err)
	}
	cmd := pbCommandTypes.Cmd{
		Cmd:     kindShardInfoForPartitions,
		Args:    args,
		Headers: headers,
	}
	cmdBytes, err := proto.Marshal(&cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	return cmdBytes, nil
}

func (c shardInfoForPartitionsEncoderDecoder) DecodeResults(_ context.Context, bytes []byte) (any, error) {
	var co pbBrokerCommand.ShardInfoForPartitionsOutputs
	if err := proto.Unmarshal(bytes, &co); err != nil {
		return nil, fmt.Errorf("unmarshal command result: %w", err)
	}
	return &co, nil
}

func (c shardInfoForPartitions) Lookup(
	ctx context.Context,
	inputs []byte,
) ([]byte, error) {
	var ci pbBrokerCommand.ShardInfoForPartitionsInputs
	if err := proto.Unmarshal(inputs, &ci); err != nil {
		return nil, fmt.Errorf("unmarshal command args: %w", err)
	}
	var partitions []*model.Partition
	for _, partition := range ci.Partitions {
		partitions = append(partitions, model.FromProtoBufPartition(partition))
	}
	shardInfoForPartitions, brokers, err := c.fsm.BrokerService().ShardInfoForPartitions(ctx, partitions)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	co := pbBrokerCommand.ShardInfoForPartitionsOutputs{
		ShardInfo: make(map[string]*pbTypes.ShardInfo),
	}
	for _, broker := range brokers {
		co.Brokers = append(co.Brokers, broker.ToProtoBuf())
	}
	for partitionID, shardInfo := range shardInfoForPartitions {
		co.ShardInfo[partitionID] = shardInfo.ToProtoBuf()
	}
	coBytes, err := proto.Marshal(&co)
	if err != nil {
		return nil, fmt.Errorf("marshal broker: %w", err)
	}
	return coBytes, nil
}
