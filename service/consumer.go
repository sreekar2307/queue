package service

import (
	"context"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service/consumer"
)

type ConsumerService interface {
	Connect(
		_ context.Context,
		_ uint64,
		groupID string,
		brokerID string,
		topics []string,
	) (
		*model.Consumer,
		*model.ConsumerGroup,
		error,
	)
	HealthCheck(context.Context, uint64, string, int64) (*model.Consumer, error)
	GetConsumer(context.Context, string) (*model.Consumer, error)
	UpdateConsumer(context.Context, uint64, *model.Consumer) (*model.Consumer, error)
	AllConsumers(context.Context) ([]*model.Consumer, error)
	Disconnect(context.Context, uint64, string) error
}

var _ ConsumerService = (*consumer.DefaultConsumerService)(nil)
