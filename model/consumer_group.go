package model

import (
	"fmt"
	pbTypes "github.com/sreekar2307/queue/gen/types/v1"
	"github.com/sreekar2307/queue/util"
)

type ConsumerGroup struct {
	ID                  string
	Topics              map[string]bool
	Consumers           map[string]bool
	rebalanceInProgress bool
}

func (c *ConsumerGroup) ToProtoBuf() *pbTypes.ConsumerGroup {
	return &pbTypes.ConsumerGroup{
		Id:        c.ID,
		Topics:    util.Keys(c.Topics),
		Consumers: util.Keys(c.Consumers),
	}
}

func FromProtoBufConsumerGroup(pb *pbTypes.ConsumerGroup) *ConsumerGroup {
	return &ConsumerGroup{
		ID:        pb.Id,
		Topics:    util.ToSet(pb.Topics),
		Consumers: util.ToSet(pb.Consumers),
	}
}

func (c *ConsumerGroup) RebalanceInProgress() bool {
	return c.rebalanceInProgress
}

func (c *ConsumerGroup) SetRebalanceInProgress(inProgress bool) {
	c.rebalanceInProgress = inProgress
}

func (c *ConsumerGroup) AddConsumer(consumerID string) {
	if c.Consumers == nil {
		c.Consumers = make(map[string]bool)
	}
	c.Consumers[consumerID] = true
}
func (c *ConsumerGroup) String() string {
	return fmt.Sprintf("id: %s, topics: %v, consumers: %v", c.ID, c.Topics, c.Consumers)
}
