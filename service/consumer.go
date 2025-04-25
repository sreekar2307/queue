package service

import (
	"context"
	"queue/model"
	"queue/service/consumer"
)

type ConsumerService interface {
	Connect(context.Context, string, string, []string) (*model.Consumer, *model.ConsumerGroup, error)
	GetConsumer(context.Context, string) (*model.Consumer, error)
	Disconnect(context.Context, string) error
}

var _ ConsumerService = (*consumer.DefaultConsumerService)(nil)
