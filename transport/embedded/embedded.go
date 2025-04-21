package embedded

import (
	"context"
	"fmt"
	"queue/model"
	"queue/service"
)

type Transport struct {
	Queue *service.Queue
}

func NewTransport(
	ctx context.Context,
	config service.Config,
) (*Transport, error) {
	queue, err := service.NewQueue(
		ctx,
		config,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}
	transport := &Transport{
		Queue: queue,
	}
	return transport, nil
}

func (t *Transport) Connect(ctx context.Context, consumerID string, consumerGroup string) error {
	// TODO implement me
	panic("implement me")
}

func (t *Transport) Close(ctx context.Context) error {
	return t.Queue.Close(ctx)
}

func (t *Transport) CreateTopic(rCtx context.Context, name string, numberOfPartitions uint64) (*model.Topic, error) {
	return t.Queue.CreateTopic(rCtx, name, numberOfPartitions)
}

func (t *Transport) SendMessage(rCtx context.Context, msg *model.Message) (*model.Message, error) {
	return t.Queue.SendMessage(rCtx, msg)
}

func (t *Transport) ReceiveMessage(ctx context.Context, topic, consumerGroup string) (*model.Message, error) {
	// TODO implement me
	panic("implement me")
}

func (t *Transport) AckMessage(ctx context.Context, topic, consumerGroup string, message *model.Message) error {
	// TODO implement me
	panic("implement me")
}
