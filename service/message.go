package service

import (
	"context"
	"io"
	"queue/model"
	messageServ "queue/service/message"
)

type MessageService interface {
	AppendMessage(context.Context, uint64, *model.Message) error
	Poll(context.Context, string, string) (*model.Message, error)
	AckMessage(context.Context, uint64, string, *model.Message) error
	Close(context.Context) error
	Open(context.Context) error
	RecoverFromSnapshot(context.Context, io.Reader) error
	Snapshot(context.Context, io.Writer) error
	LastAppliedCommandID(ctx context.Context) (uint64, error)
}

var _ MessageService = (*messageServ.DefaultMessageService)(nil)
