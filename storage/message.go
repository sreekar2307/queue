package storage

import (
	"context"
	"queue/model"
)

type MessageStorage interface {
	AppendMessage(context.Context, *model.Message) error
	MessageAtIndex(context.Context, *model.Partition, []byte) (*model.Message, error)
	Close(context.Context) error
	AckMessage(context.Context, *model.Message, *model.ConsumerGroup) error
	NextUnAckedMessageID(context.Context, *model.Partition, *model.ConsumerGroup) ([]byte, error)
	LastMessageID(context.Context, string) ([]byte, error)
}
