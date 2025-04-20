package model

import "github.com/lni/dragonboat/v4"

type Broker struct {
	ID            uint64
	nh            *dragonboat.NodeHost
	brokerShardId uint64
}

func (b *Broker) BrokerShardId() uint64 {
	return b.brokerShardId
}

func (b *Broker) SetBrokerShardId(brokerShardId uint64) {
	b.brokerShardId = brokerShardId
}

func (b *Broker) SetNodeHost(nh *dragonboat.NodeHost) {
	b.nh = nh
}

func (b *Broker) NodeHost() *dragonboat.NodeHost {
	return b.nh
}
