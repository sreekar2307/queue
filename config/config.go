package config

import (
	"fmt"
	"time"
)

type (
	Config struct {
		MetadataPath   string
		PartitionsPath string
		RaftConfig     *RaftConfig

		ShardLeaderWaitTime         time.Duration
		ReCheckInterval             time.Duration
		ConsumerLostTime            time.Duration
		ConsumerHealthCheckInterval time.Duration

		GRPC *GRPCConfig
		HTTP *HTTPConfig
	}

	GRPCConfig struct {
		ListenerAddr string
	}

	HTTPConfig struct {
		ListenerAddr string
	}
)

type RaftConfig struct {
	ReplicaID       uint64
	RaftNodeAddr    string
	InviteMembers   map[uint64]string
	RaftLogsDataDir string
}

var DefaultConfig = Config{
	ShardLeaderWaitTime:         30 * time.Second,
	ConsumerLostTime:            30 * time.Second,
	ConsumerHealthCheckInterval: 1 * time.Second,
}

func (c *Config) WithDefaults() {
	if c.ShardLeaderWaitTime == 0 {
		c.ShardLeaderWaitTime = DefaultConfig.ShardLeaderWaitTime
	}
	if c.ConsumerLostTime == 0 {
		c.ConsumerLostTime = DefaultConfig.ConsumerLostTime
	}
	if c.ConsumerHealthCheckInterval == 0 {
		c.ConsumerHealthCheckInterval = DefaultConfig.ConsumerHealthCheckInterval
	}
}

func (c *Config) Validate() error {
	if c.MetadataPath == "" {
		return fmt.Errorf("metadata path is required")
	}
	if c.PartitionsPath == "" {
		return fmt.Errorf("partitions path is required")
	}
	if c.RaftConfig == nil {
		return fmt.Errorf("raft config is required")
	}
	if c.ConsumerLostTime == 0 {
		return fmt.Errorf("consumer lost time is required")
	}
	if c.ShardLeaderWaitTime == 0 {
		return fmt.Errorf("shard leader wait time is required")
	}
	if c.ConsumerHealthCheckInterval == 0 {
		return fmt.Errorf("consumer health check interval is required")
	}
	if err := c.RaftConfig.Validate(); err != nil {
		return err
	}
	return nil
}

func (c *RaftConfig) Validate() error {
	if c.RaftNodeAddr == "" {
		return fmt.Errorf("raft node address is required")
	}
	if c.ReplicaID == 0 {
		return fmt.Errorf("replica ID is required")
	}
	if len(c.InviteMembers) == 0 {
		return fmt.Errorf("invite members are required")
	}
	if c.RaftLogsDataDir == "" {
		return fmt.Errorf("raft logs data dir is required")
	}
	return nil
}
