package distributedQueue

import (
	"context"
	"fmt"
	"queue/internal/parition"
	"queue/internal/parition/selection"
	"queue/internal/topic"
	"queue/message"
	"sync"
)

type Queue struct {
	topics map[string]*topic.Topic

	mu sync.Mutex
}

func NewQueue() *Queue {
	return &Queue{
		topics: make(map[string]*topic.Topic),
	}
}

func (q *Queue) CreateTopic(ctx context.Context, name string) (*topic.Topic, error) {
	if _, ok := q.topics[name]; ok {
		return nil, fmt.Errorf("topic '%s' already exists", name)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.topics[name] = topic.NewTopic(ctx, name, selection.NewRoundRobinPartitionSelectionStrategy())
	return q.topics[name], nil
}

func (q *Queue) SendMessage(ctx context.Context, topic string, msg *message.Message) (*message.Message, error) {
	if _, ok := q.topics[topic]; !ok {
		return nil, fmt.Errorf("topic '%s' does not exist", topic)
	}
	return q.topics[topic].SendMessage(ctx, parition.DefaultPartition, msg)
}

func (q *Queue) SendMessageToPartition(ctx context.Context, topic, partition string, msg *message.Message) (*message.Message, error) {
	if _, ok := q.topics[topic]; !ok {
		return nil, fmt.Errorf("topic '%s' does not exist", topic)
	}
	return q.topics[topic].SendMessage(ctx, partition, msg)
}

func (q *Queue) ReceiveMessage(ctx context.Context, topic, consumerGroup string) (*message.Message, error) {
	if _, ok := q.topics[topic]; !ok {
		return nil, fmt.Errorf("topic '%s' does not exist", topic)
	}
	return q.topics[topic].ReceiveMessage(ctx, consumerGroup)
}

func (q *Queue) AckMessage(ctx context.Context, topic, consumerGroup string, message *message.Message) error {
	if _, ok := q.topics[topic]; !ok {
		return fmt.Errorf("topic '%s' does not exist", topic)
	}
	return q.topics[topic].AckMessage(ctx, message, consumerGroup)
}
