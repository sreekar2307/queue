package service

import (
	"context"
	"queue/service/consumer"
)

type ConsumerService interface {
	Connect(context.Context, string, string, []string) error
	Disconnect(context.Context, string) error
}

var _ ConsumerService = (*consumer.DefaultConsumerService)(nil)
