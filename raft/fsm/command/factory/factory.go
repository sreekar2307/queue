package factory

import (
	"github.com/sreekar2307/queue/raft/fsm/command"
	"github.com/sreekar2307/queue/raft/fsm/command/broker/update"
)

type factory struct {
	updates map[command.Kind]command.Builder
	lookups map[command.Kind]command.Builder
}

func init() {
	RegisterUpdateBuilder(update.NewCreateTopicBuilder())
}

var defaultFactory = &factory{
	updates: make(map[command.Kind]command.Builder),
	lookups: make(map[command.Kind]command.Builder),
}

func RegisterUpdateBuilder(cmd command.Builder) {
	defaultFactory.RegisterUpdateBuilder(cmd)
}

func RegisterBuilder(cmd command.Builder) {
	defaultFactory.RegisterBuilder(cmd)
}

func GetBrokerExecuteUpdate(k command.Kind, fsm command.BrokerFSM) (command.Update, error) {
	return defaultFactory.GetBrokerExecuteUpdate(k, fsm)
}

func GetBrokerEncoderDecoder(k command.Kind) (command.EncoderDecoder, error) {
	return defaultFactory.GetBrokerEncoderDecoder(k)
}

func GetBrokerLookup(k command.Kind, fsm command.BrokerFSM) (command.Lookup, error) {
	return defaultFactory.GetLookup(k, fsm)
}

func (f *factory) RegisterUpdateBuilder(cmd command.Builder) {
	f.updates[cmd.Kind()] = cmd
}

func (f *factory) RegisterBuilder(cmd command.Builder) {
	f.lookups[cmd.Kind()] = cmd
}

func (f *factory) GetBrokerExecuteUpdate(k command.Kind, fsm command.BrokerFSM) (command.Update, error) {
	if f.updates == nil {
		return nil, ErrNoCommandsRegistered
	}
	cmdBuilder, ok := f.updates[k]
	if !ok {
		return nil, ErrCommandNotFound
	}
	cmdBrokerBuilder, ok := cmdBuilder.(command.UpdateBrokerBuilder)
	if !ok {
		return nil, ErrCommandNotFound
	}
	return cmdBrokerBuilder.NewUpdate(fsm), nil
}

func (f *factory) GetBrokerEncoderDecoder(k command.Kind) (command.EncoderDecoder, error) {
	if f.updates == nil {
		return nil, ErrNoCommandsRegistered
	}
	cmdBuilder, ok := f.updates[k]
	if !ok {
		return nil, ErrCommandNotFound
	}
	cmdBrokerBuilder, ok := cmdBuilder.(command.Builder)
	if !ok {
		return nil, ErrCommandNotFound
	}
	return cmdBrokerBuilder.NewEncoderDecoder(), nil
}

func (f *factory) GetLookup(k command.Kind, fsm command.BrokerFSM) (command.Lookup, error) {
	if f.lookups == nil {
		return nil, ErrNoCommandsRegistered
	}
	cmdBuilder, ok := f.lookups[k]
	if !ok {
		return nil, ErrCommandNotFound
	}
	cmdBrokerBuilder, ok := cmdBuilder.(command.LookupBuilder)
	if !ok {
		return nil, ErrCommandNotFound
	}
	return cmdBrokerBuilder.NewLookup(nil), nil
}
