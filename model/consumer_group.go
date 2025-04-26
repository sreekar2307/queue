package model

import "fmt"

type ConsumerGroup struct {
	ID                  string
	Topics              map[string]bool
	Consumers           map[string]bool
	rebalanceInProgress bool
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
