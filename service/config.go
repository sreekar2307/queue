package service

import "fmt"

type Config struct {
	RaftNodeAddr    string
	ReplicaID       uint64
	InviteMembers   map[uint64]string
	RaftLogsDataDir string

	MetadataPath   string
	PartitionsPath string
}

func (c *Config) Validate() error {
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
	if c.MetadataPath == "" {
		return fmt.Errorf("metadata path is required")
	}
	if c.PartitionsPath == "" {
		return fmt.Errorf("partitions path is required")
	}
	return nil
}
