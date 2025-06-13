package command

import (
	"context"

	"github.com/lni/dragonboat/v4/statemachine"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service"
)

type (
	Kind string
	Cmd  struct {
		CommandType Kind
		Args        [][]byte
	}

	Builder interface {
		Kind() pbCommandTypes.Kind
		NewEncoderDecoder() EncoderDecoder
	}

	UpdateMessageBuilder interface {
		Builder
		NewUpdate(MessageFSM) Update
	}
	UpdateBrokerBuilder interface {
		Builder
		NewUpdate(BrokerFSM) Update
	}

	EncoderDecoder interface {
		EncodeArgs(context.Context, any) ([]byte, error)
		DecodeResults(context.Context, []byte) (any, error)
	}

	Update interface {
		ExecuteUpdate(context.Context, []byte, statemachine.Entry) (statemachine.Entry, error)
	}

	Lookup interface {
		Lookup(context.Context, []byte) ([]byte, error)
	}

	LookupBrokerBuilder interface {
		Builder
		NewLookup(BrokerFSM) Lookup
	}

	LookupMessageBuilder interface {
		Builder
		NewLookup(fsm MessageFSM) Lookup
	}

	BrokerFSM interface {
		TopicService() service.TopicService
		BrokerService() service.BrokerService
		ConsumerService() service.ConsumerService
		Broker() *model.Broker
	}

	MessageFSM interface {
		MessageService() service.MessageService
		Broker() *model.Broker
	}
)
