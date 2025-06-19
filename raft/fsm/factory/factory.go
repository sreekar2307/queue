package factory

import (
	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/sreekar2307/queue/assignor/sticky"
	"github.com/sreekar2307/queue/config"
	"github.com/sreekar2307/queue/logger"
	"github.com/sreekar2307/queue/model"
	brokerFSM "github.com/sreekar2307/queue/raft/fsm/broker"
	"github.com/sreekar2307/queue/raft/fsm/message"
	brokerServ "github.com/sreekar2307/queue/service/broker"
	consumerServ "github.com/sreekar2307/queue/service/consumer"
	messageServ "github.com/sreekar2307/queue/service/message"
	topicServ "github.com/sreekar2307/queue/service/topic"
	"github.com/sreekar2307/queue/storage"
	messageStorage "github.com/sreekar2307/queue/storage/message"
	"path/filepath"
	"strconv"
)

func NewMessageFSM(
	shardID, replicaID uint64,
	broker *model.Broker,
	log logger.Logger,
	mdStorage storage.MetadataStorage,
) statemachine.IOnDiskStateMachine {
	var (
		partitionsStorePath = filepath.Join(
			config.Conf.PartitionsPath,
			strconv.Itoa(int(shardID)),
			strconv.Itoa(int(replicaID)),
		)
		messageService = messageServ.NewMessageService(
			messageStorage.NewBolt(
				partitionsStorePath,
				log,
			),
			mdStorage,
			partitionsStorePath,
			broker,
			log,
		)
		fsm = new(message.FSM)
	)
	fsm.SetLog(log)
	fsm.SetMessageService(messageService)
	fsm.SetBroker(broker)
	fsm.SetShardID(shardID)
	fsm.SetReplicaID(replicaID)
	return fsm
}

func NewBrokerFSM(
	shardID, replicaID uint64,
	broker *model.Broker,
	log logger.Logger,
	mdStorage storage.MetadataStorage,
) statemachine.IOnDiskStateMachine {
	var (
		topicService = topicServ.NewTopicService(
			mdStorage,
			log,
		)
		consumerService = consumerServ.NewConsumerService(
			mdStorage,
			sticky.NewAssignor(mdStorage),
		)
		brokerService = brokerServ.NewBrokerService(
			mdStorage,
		)
		fsm = new(brokerFSM.FSM)
	)
	fsm.SetLog(log)
	fsm.SetShardID(shardID)
	fsm.SetReplicaID(replicaID)
	fsm.SetMdStorage(mdStorage)
	fsm.SetBroker(broker)
	fsm.SetTopicService(topicService)
	fsm.SetConsumerService(consumerService)
	fsm.SetBrokerService(brokerService)
	return fsm
}
