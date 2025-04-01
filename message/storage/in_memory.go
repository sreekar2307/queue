package storage

import (
	"context"
	"queue/message"
	"sync"
)

type inMemory struct {
	messages []*message.Message

	mu sync.RWMutex
}

func NewInMemoryStorage() Storage {
	return &inMemory{
		messages: make([]*message.Message, 0),
	}
}

func (i *inMemory) WriteMessage(_ context.Context, msg *message.Message) (*message.Message, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	msg.SetMessageID(len(i.messages))
	i.messages = append(i.messages, msg)
	return msg, nil
}

func (i *inMemory) ReadMessage(_ context.Context, id int) (*message.Message, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if id >= len(i.messages) {
		return nil, nil
	}
	return i.messages[id], nil
}
