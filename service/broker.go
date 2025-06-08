package service

import (
	"context"

	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service/broker"
)

type BrokerService interface {
	RegisterBroker(context.Context, uint64, *model.Broker) (*model.Broker, error)
	ShardInfoForPartitions(context.Context, []*model.Partition) (map[string]*model.ShardInfo, []*model.Broker, error)
}

var _ BrokerService = (*broker.DefaultBroker)(nil)
