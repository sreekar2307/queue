package factory

import (
	"github.com/sreekar2307/queue/raft/fsm/command"
	"github.com/sreekar2307/queue/raft/fsm/command/broker/lookup"
	"github.com/sreekar2307/queue/raft/fsm/command/broker/update"
	"sync"
)

var mu sync.RWMutex

type factory struct {
	brokerUpdates  map[command.Kind]command.UpdateBrokerBuilder
	brokerLookups  map[command.Kind]command.Builder
	messageUpdates map[command.Kind]command.UpdateMessageBuilder
	messageLookups map[command.Kind]command.Builder
	builders       map[command.Kind]command.Builder
}

func init() {
	RegisterBrokerUpdateBuilder(update.NewCreateTopicBuilder())
	RegisterBrokerLookupBuilder(lookup.NewTopicForIDBuilder())
}

var defaultFactory = &factory{
	brokerUpdates:  make(map[command.Kind]command.UpdateBrokerBuilder),
	brokerLookups:  make(map[command.Kind]command.Builder),
	messageUpdates: make(map[command.Kind]command.UpdateMessageBuilder),
	messageLookups: make(map[command.Kind]command.Builder),
	builders:       make(map[command.Kind]command.Builder),
}

func RegisterBrokerUpdateBuilder(cmd command.UpdateBrokerBuilder) {
	mu.Lock()
	defer mu.Unlock()
	defaultFactory.RegisterBrokerUpdateBuilder(cmd)
}

func RegisterBrokerLookupBuilder(cmd command.LookupBrokerBuilder) {
	mu.Lock()
	defer mu.Unlock()
	defaultFactory.RegisterBrokerLookupBuilder(cmd)
}

func BrokerExecuteUpdate(k command.Kind, fsm command.BrokerFSM) (command.Update, error) {
	return defaultFactory.BrokerExecuteUpdate(k, fsm)
}

func BrokerEncoderDecoder(k command.Kind) (command.EncoderDecoder, error) {
	return defaultFactory.BrokerEncoderDecoder(k)
}

func BrokerLookup(k command.Kind, fsm command.BrokerFSM) (command.Lookup, error) {
	return defaultFactory.BrokerLookup(k, fsm)
}

func (f *factory) RegisterBrokerUpdateBuilder(cmd command.UpdateBrokerBuilder) {
	f.brokerUpdates[cmd.Kind()] = cmd
	f.builders[cmd.Kind()] = cmd
}

func (f *factory) RegisterBrokerLookupBuilder(cmd command.LookupBrokerBuilder) {
	f.brokerLookups[cmd.Kind()] = cmd
	f.builders[cmd.Kind()] = cmd
}

func (f *factory) BrokerExecuteUpdate(k command.Kind, fsm command.BrokerFSM) (command.Update, error) {
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
	return cmdBrokerBuilder.NewUpdate(fsm), nil
}

func (f *factory) BrokerEncoderDecoder(k command.Kind) (command.EncoderDecoder, error) {
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

func (f *factory) BrokerLookup(k command.Kind, fsm command.BrokerFSM) (command.Lookup, error) {
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
	return cmdBrokerBuilder.NewLookup(fsm), nil
}
