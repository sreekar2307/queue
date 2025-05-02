package storage

import (
	"context"
	"io"
	"queue/model"
)

type MessageStorage interface {
	AppendMessage(context.Context, uint64, *model.Message) error
	MessageAtIndex(context.Context, *model.Partition, []byte) (*model.Message, error)
	Close(context.Context) error
	AckMessage(context.Context, uint64, *model.Message, *model.ConsumerGroup) error
	NextUnAckedMessageID(context.Context, *model.Partition, *model.ConsumerGroup) ([]byte, error)
	LastMessageID(context.Context, string) ([]byte, error)
	Snapshot(context.Context, io.Writer) error
	RecoverFromSnapshot(context.Context, io.Reader) error
}
