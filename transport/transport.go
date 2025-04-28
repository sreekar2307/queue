package transport

import (
	"context"
	"queue/model"
	"time"
)

type Transport interface {
	Start(ctx context.Context) error
	Connect(ctx context.Context, consumerID string, consumerGroup string, topics []string) (*model.Consumer, *model.ConsumerGroup, error)
	Close(ctx context.Context) error
	CreateTopic(ctx context.Context, name string, numerOfParititions uint64) (*model.Topic, error)
	SendMessage(ctx context.Context, msg *model.Message) (*model.Message, error)
	ReceiveMessage(ctx context.Context, consumerGroup string) (*model.Message, error)
	AckMessage(ctx context.Context, cosumerID string, message *model.Message) error
	HealthCheck(context.Context, string, time.Time) (*model.Consumer, error)
}
