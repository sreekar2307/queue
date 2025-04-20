package model

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
