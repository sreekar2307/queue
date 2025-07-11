package command

import (
	"context"
	"github.com/sreekar2307/queue/logger"

	"github.com/lni/dragonboat/v4/statemachine"
	pbCommandTypes "github.com/sreekar2307/queue/gen/raft/fsm/v1"
	"github.com/sreekar2307/queue/model"
	"github.com/sreekar2307/queue/service"
	"github.com/sreekar2307/queue/storage"
)

type (
	Builder interface {
		Kind() pbCommandTypes.Kind
		NewEncoderDecoder() EncoderDecoder
	}

	EncoderDecoder interface {
		EncodeArgs(context.Context, any, map[string]string) ([]byte, error)
		DecodeResults(context.Context, []byte) (any, error)
	}

	UpdateMessageBuilder interface {
		Builder
		NewUpdate(MessageFSM, logger.Logger) Update
	}
	UpdateBrokerBuilder interface {
		Builder
		NewUpdate(BrokerFSM, logger.Logger) Update
	}

	Update interface {
		ExecuteUpdate(context.Context, []byte, statemachine.Entry) (statemachine.Entry, error)
	}

	LookupBrokerBuilder interface {
		Builder
		NewLookup(BrokerFSM, logger.Logger) Lookup
	}

	LookupMessageBuilder interface {
		Builder
		NewLookup(MessageFSM, logger.Logger) Lookup
	}

	Lookup interface {
		Lookup(context.Context, []byte) ([]byte, error)
	}

	BrokerFSM interface {
		TopicService() service.TopicService
		BrokerService() service.BrokerService
		ConsumerService() service.ConsumerService
		Broker() *model.Broker
		MdStorage() storage.MetadataStorage
	}

	MessageFSM interface {
		MessageService() service.MessageService
		Broker() *model.Broker
	}
)
