package factory

import (
	"github.com/sreekar2307/queue/logger"
	"sync"

	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/raft/fsm/command"
	brokerLookup "github.com/sreekar2307/queue/raft/fsm/command/broker/lookup"
	brokerUpdate "github.com/sreekar2307/queue/raft/fsm/command/broker/update"
	messageLookup "github.com/sreekar2307/queue/raft/fsm/command/message/lookup"
	messageUpdate "github.com/sreekar2307/queue/raft/fsm/command/message/update"
)

var mu sync.RWMutex

type factory struct {
	brokerUpdates  map[pbCommandTypes.Kind]command.UpdateBrokerBuilder
	brokerLookups  map[pbCommandTypes.Kind]command.Builder
	messageUpdates map[pbCommandTypes.Kind]command.UpdateMessageBuilder
	messageLookups map[pbCommandTypes.Kind]command.Builder
	builders       map[pbCommandTypes.Kind]command.Builder
}

func init() {
	RegisterBrokerUpdateBuilder(brokerUpdate.NewCreateTopicBuilder())
	RegisterBrokerUpdateBuilder(brokerUpdate.NewDisconnectBuilder())
	RegisterBrokerUpdateBuilder(brokerUpdate.NewConnectBuilder())
	RegisterBrokerUpdateBuilder(brokerUpdate.NewRegisterBrokerBuilder())
	RegisterBrokerUpdateBuilder(brokerUpdate.NewUpdateConsumerBuilder())
	RegisterBrokerUpdateBuilder(brokerUpdate.NewHealthCheckBuilder())
	RegisterBrokerUpdateBuilder(brokerUpdate.NewPartitionAddedBuilder())

	RegisterBrokerLookupBuilder(brokerLookup.NewAllPartitionsBuilder())
	RegisterBrokerLookupBuilder(brokerLookup.NewBrokerForIDBuilder())
	RegisterBrokerLookupBuilder(brokerLookup.NewConsumersBuilder())
	RegisterBrokerLookupBuilder(brokerLookup.NewConsumerForIDBuilder())
	RegisterBrokerLookupBuilder(brokerLookup.NewPartitionIDForMessageBuilder())
	RegisterBrokerLookupBuilder(brokerLookup.NewPartitionsForTopicBuilder())
	RegisterBrokerLookupBuilder(brokerLookup.NewShardInfoForPartitionsBuilder())
	RegisterBrokerLookupBuilder(brokerLookup.NewTopicForIDBuilder())

	RegisterMessageUpdateBuilder(messageUpdate.NewAckBuilder())
	RegisterMessageUpdateBuilder(messageUpdate.NewAppendBuilder())

	RegisterMessageLookupBuilder(messageLookup.NewPollBuilder())
}

var defaultFactory = &factory{
	brokerUpdates:  make(map[pbCommandTypes.Kind]command.UpdateBrokerBuilder),
	brokerLookups:  make(map[pbCommandTypes.Kind]command.Builder),
	messageUpdates: make(map[pbCommandTypes.Kind]command.UpdateMessageBuilder),
	messageLookups: make(map[pbCommandTypes.Kind]command.Builder),
	builders:       make(map[pbCommandTypes.Kind]command.Builder),
}

func RegisterBrokerUpdateBuilder(cmd command.UpdateBrokerBuilder) {
	mu.Lock()
	defer mu.Unlock()
	defaultFactory.RegisterBrokerUpdateBuilder(cmd)
}

func RegisterMessageUpdateBuilder(cmd command.UpdateMessageBuilder) {
	mu.Lock()
	defer mu.Unlock()
	defaultFactory.RegisterMessageUpdateBuilder(cmd)
}

func RegisterBrokerLookupBuilder(cmd command.LookupBrokerBuilder) {
	mu.Lock()
	defer mu.Unlock()
	defaultFactory.RegisterBrokerLookupBuilder(cmd)
}

func RegisterMessageLookupBuilder(cmd command.LookupMessageBuilder) {
	mu.Lock()
	defer mu.Unlock()
	defaultFactory.RegisterMessageLookupBuilder(cmd)
}

func BrokerExecuteUpdate(k pbCommandTypes.Kind, fsm command.BrokerFSM, log logger.Logger) (command.Update, error) {
	return defaultFactory.BrokerExecuteUpdate(k, fsm, log)
}

func MessageExecuteUpdate(k pbCommandTypes.Kind, fsm command.MessageFSM, log logger.Logger) (command.Update, error) {
	return defaultFactory.MessageExecuteUpdate(k, fsm, log)
}

func EncoderDecoder(k pbCommandTypes.Kind) (command.EncoderDecoder, error) {
	return defaultFactory.EncoderDecoder(k)
}

func BrokerLookup(k pbCommandTypes.Kind, fsm command.BrokerFSM, log logger.Logger) (command.Lookup, error) {
	return defaultFactory.BrokerLookup(k, fsm, log)
}

func MessageLookup(k pbCommandTypes.Kind, fsm command.MessageFSM, log logger.Logger) (command.Lookup, error) {
	return defaultFactory.MessageLookup(k, fsm, log)
}

func (f *factory) RegisterBrokerUpdateBuilder(cmd command.UpdateBrokerBuilder) {
	f.brokerUpdates[cmd.Kind()] = cmd
	f.builders[cmd.Kind()] = cmd
}

func (f *factory) RegisterBrokerLookupBuilder(cmd command.LookupBrokerBuilder) {
	f.brokerLookups[cmd.Kind()] = cmd
	f.builders[cmd.Kind()] = cmd
}

func (f *factory) BrokerExecuteUpdate(k pbCommandTypes.Kind, fsm command.BrokerFSM, log logger.Logger) (command.Update, error) {
	if f.brokerUpdates == nil {
		return nil, ErrNoCommandsRegistered
	}
	cmdBuilder, ok := f.brokerUpdates[k]
	if !ok {
		return nil, ErrCommandNotFound
	}
	cmdBrokerBuilder, ok := cmdBuilder.(command.UpdateBrokerBuilder)
	if !ok {
		return nil, ErrCommandNotFound
	}
	return cmdBrokerBuilder.NewUpdate(fsm, log), nil
}

func (f *factory) EncoderDecoder(k pbCommandTypes.Kind) (command.EncoderDecoder, error) {
	mu.RLock()
	defer mu.RUnlock()
	if f.builders == nil {
		return nil, ErrNoCommandsRegistered
	}
	cmdBuilder, ok := f.builders[k]
	if !ok {
		return nil, ErrCommandNotFound
	}
	cmdBrokerBuilder, ok := cmdBuilder.(command.Builder)
	if !ok {
		return nil, ErrCommandNotFound
	}
	return cmdBrokerBuilder.NewEncoderDecoder(), nil
}

func (f *factory) BrokerLookup(k pbCommandTypes.Kind, fsm command.BrokerFSM, log logger.Logger) (command.Lookup, error) {
	mu.RLock()
	defer mu.RUnlock()
	if f.brokerLookups == nil {
		return nil, ErrNoCommandsRegistered
	}
	cmdBuilder, ok := f.brokerLookups[k]
	if !ok {
		return nil, ErrCommandNotFound
	}
	cmdBrokerBuilder, ok := cmdBuilder.(command.LookupBrokerBuilder)
	if !ok {
		return nil, ErrCommandNotFound
	}
	return cmdBrokerBuilder.NewLookup(fsm, log), nil
}

func (f *factory) RegisterMessageUpdateBuilder(cmd command.UpdateMessageBuilder) {
	f.messageUpdates[cmd.Kind()] = cmd
	f.builders[cmd.Kind()] = cmd
}

func (f *factory) RegisterMessageLookupBuilder(cmd command.LookupMessageBuilder) {
	f.messageLookups[cmd.Kind()] = cmd
	f.builders[cmd.Kind()] = cmd
}

func (f *factory) MessageExecuteUpdate(k pbCommandTypes.Kind, fsm command.MessageFSM, l logger.Logger) (command.Update, error) {
	if f.messageUpdates == nil {
		return nil, ErrNoCommandsRegistered
	}
	cmdBuilder, ok := f.messageUpdates[k]
	if !ok {
		return nil, ErrCommandNotFound
	}
	cmdBrokerBuilder, ok := cmdBuilder.(command.UpdateMessageBuilder)
	if !ok {
		return nil, ErrCommandNotFound
	}
	return cmdBrokerBuilder.NewUpdate(fsm, l), nil
}

func (f *factory) MessageLookup(k pbCommandTypes.Kind, fsm command.MessageFSM, l logger.Logger) (command.Lookup, error) {
	mu.RLock()
	defer mu.RUnlock()
	if f.messageLookups == nil {
		return nil, ErrNoCommandsRegistered
	}
	cmdBuilder, ok := f.messageLookups[k]
	if !ok {
		return nil, ErrCommandNotFound
	}
	cmdBrokerBuilder, ok := cmdBuilder.(command.LookupMessageBuilder)
	if !ok {
		return nil, ErrCommandNotFound
	}
	return cmdBrokerBuilder.NewLookup(fsm, l), nil
}
