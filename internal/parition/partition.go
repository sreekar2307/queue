package parition

import (
	"fmt"
	"queue/message"
	"sync"
)

type Partition struct {
	name               string
	messages           []*message.Message
	consumerGroupIndex map[string]int

	messagesMu      sync.RWMutex
	consumerGroupMu sync.RWMutex
}

var NoNewMessageErr = fmt.Errorf("no new messages")

func NewPartition(name string) *Partition {
	return &Partition{
		name:               name,
		messages:           make([]*message.Message, 0),
		consumerGroupIndex: make(map[string]int),
	}
}

func (p *Partition) WriteMessage(message *message.Message) (*message.Message, error) {
	p.messagesMu.Lock()
	defer p.messagesMu.Unlock()
	message.SetMessageID(len(p.messages))
	message.SetPartitionKey(p.name)
	p.messages = append(p.messages, message)
	return message, nil
}

func (p *Partition) ReadMessage(consumerGroup string) (*message.Message, error) {
	p.consumerGroupMu.RLock()
	id := p.consumerGroupIndex[consumerGroup]
	p.consumerGroupMu.RUnlock()

	p.messagesMu.RLock()
	defer p.messagesMu.RUnlock()
	if id >= len(p.messages) {
		return nil, NoNewMessageErr
	}
	return p.messages[id], nil
}

func (p *Partition) AckMessage(msg *message.Message, consumerGroup string) error {
	p.consumerGroupMu.Lock()
	defer p.consumerGroupMu.Unlock()
	p.consumerGroupIndex[consumerGroup] = msg.MessageID() + 1
	return nil
}
