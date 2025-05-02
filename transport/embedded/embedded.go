package embedded

import (
	"context"
	"fmt"
	"queue/model"
	"queue/service"
)

type Embedded struct {
	queue *service.Queue
}

func NewTransport(
	_ context.Context,
	queue *service.Queue,
) (*Embedded, error) {
	transport := &Embedded{
		queue: queue,
	}
	return transport, nil
}

func (e *Embedded) Start(_ context.Context) error {
	return nil
}

func (e *Embedded) Connect(
	ctx context.Context,
	consumerID string,
	consumerGroup string,
	topics []string,
) (*model.Consumer, *model.ConsumerGroup, error) {
	consumer, group, err := e.queue.Connect(ctx, consumerID, consumerGroup, topics)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect: %w", err)
	}
	return consumer, group, nil
}

func (e *Embedded) Close(ctx context.Context) error {
	return e.queue.Close(ctx)
}

func (e *Embedded) CreateTopic(pCtx context.Context, name string, numberOfPartitions uint64) (*model.Topic, error) {
	return e.queue.CreateTopic(pCtx, name, numberOfPartitions)
}

func (e *Embedded) SendMessage(pCtx context.Context, msg *model.Message) (*model.Message, error) {
	return e.queue.SendMessage(pCtx, msg)
}

func (e *Embedded) ReceiveMessage(ctx context.Context, consumerID string) (*model.Message, error) {
	return e.queue.ReceiveMessage(ctx, consumerID)
}

func (e *Embedded) AckMessage(ctx context.Context, consumerID string, message *model.Message) error {
	return e.queue.AckMessage(ctx, consumerID, message)
}
