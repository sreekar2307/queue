package transport

import (
	"context"
	"queue/model"
)

type Transport interface {
	Connect(ctx context.Context, consumerID string, consumerGroup string) error
	Close(ctx context.Context) error
	CreateTopic(ctx context.Context, name string, numerOfParititions uint64) (*model.Topic, error)
	SendMessage(ctx context.Context, msg *model.Message) (*model.Message, error)
	ReceiveMessage(ctx context.Context, topic, consumerGroup string) (*model.Message, error)
	AckMessage(ctx context.Context, topic, consumerGroup string, message *model.Message) error
}
