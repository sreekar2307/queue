package config

import (
	stdErrors "errors"
	"flag"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type (
	Config struct {
		MetadataPath   string      `mapstructure:"metadata_path"`
		PartitionsPath string      `mapstructure:"partitions_path"`
		RaftConfig     *RaftConfig `mapstructure:"raft"`

		ShardLeaderWaitTime           time.Duration `mapstructure:"shard_leader_wait_time"`
		ShardLeaderSetReCheckInterval time.Duration `mapstructure:"shard_leader_set_recheck_interval"`
		ConsumerLostTime              time.Duration `mapstructure:"consumer_lost_time"`
		ConsumerHealthCheckInterval   time.Duration `mapstructure:"consumer_health_check_interval"`
		ShutdownTimeout               time.Duration `mapstructure:"shutdown_timeout"`

		GRPC GRPC `mapstructure:"grpc"`
		HTTP HTTP `mapstructure:"http"`
	}

	GRPC struct {
		ListenerAddr string `mapstructure:"listener_addr"`
	}

	HTTP struct {
		ListenerAddr string `mapstructure:"listener_addr"`
	}

	RaftConfig struct {
		ReplicaID     uint64            `mapstructure:"replica_id"`
		Addr          string            `mapstructure:"addr"`
		Join          bool              `mapstructure:"join"`
		InviteMembers map[uint64]string `mapstructure:"invite_members"`
		LogsDataDir   string            `mapstructure:"logs_data_dir"`
		Metadata      MetadataFSMConfig `mapstructure:"metadata_fsm"`
		Messages      MessagesFSMConfig `mapstructure:"messages_fsm"`
	}

	MetadataFSMConfig struct {
		SnapshotEntries    uint64 `mapstructure:"snapshot_entries"`
		CompactionOverhead uint64 `mapstructure:"compaction_overhead"`
	}

	MessagesFSMConfig struct {
		SnapshotsEntries   uint64 `mapstructure:"snapshot_entries"`
		CompactionOverhead uint64 `mapstructure:"compaction_overhead"`
	}
)

var (
	Conf *Config

	once sync.Once
)

func init() {
	once.Do(func() {
		v := viper.New()
		v.SetEnvPrefix("QUEUE")
		v.AutomaticEnv()

		pflag.Int("raft.replica_id", 1, "ReplicaID to use")
		pflag.String("config", "config", "Path to config file")
		pflag.String("raft.addr", "localhost:63001", "Raft Nodehost address")
		pflag.String("grpc.listener_addr", "", "GRPC listener address")
		pflag.String("http.listener_addr", "", "HTTP listener address")
		pflag.Bool("raft.join", false, "Join the Raft cluster if true")
		pflag.Parse()
		pflag.CommandLine.AddGoFlagSet(flag.CommandLine)

		if err := v.BindPFlags(pflag.CommandLine); err != nil {
			panic(fmt.Errorf("failed to bind flags: %w", err))
		}
		pathToConfigFile := pflag.Lookup("config").Value.String()
		filename := filepath.Base(pathToConfigFile)

		v.AddConfigPath(filepath.Dir(pathToConfigFile))
		v.SetConfigName(filename[0 : len(filename)-len(filepath.Ext(filename))])

		if fileExt := filepath.Ext(pathToConfigFile); len(fileExt) > 1 {
			v.SetConfigType(filepath.Ext(pathToConfigFile)[1:])
		}

		v.SetDefault("raft.replica_id", 1)
		v.SetDefault("metadata_path", "metadata")
		v.SetDefault("partitions_path", "partitions")
		v.SetDefault("raft.logs_data_dir", "raft")
		v.SetDefault("raft.invite_members", map[uint64]string{
			v.GetUint64("raft.replica_id"): v.GetString("raft.addr"),
		})

		v.SetDefault("raft.metadata_fsm.snapshot_entries", 1000)
		v.SetDefault("raft.metadata_fsm.compaction_overhead", 50)
		v.SetDefault("raft.messages_fsm.snapshot_entries", 1000)
		v.SetDefault("raft.messages_fsm.compaction_overhead", 50)

		v.SetDefault("shard_leader_wait_time", 30*time.Second)
		v.SetDefault("shard_leader_set_recheck_interval", 1*time.Second)

		v.SetDefault("consumer_lost_time", 30*time.Second)
		v.SetDefault("consumer_health_check_interval", 5*time.Second)

		if err := v.ReadInConfig(); err != nil {
			if !stdErrors.As(err, &viper.ConfigFileNotFoundError{}) {
				panic(fmt.Errorf("failed to read config: %w", err))
			}
		}

		Conf = &Config{}
		if err := v.Unmarshal(Conf); err != nil {
			panic(fmt.Errorf("failed to unmarshal config: %w", err))
		}
	})
}
