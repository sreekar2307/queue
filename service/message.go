package service

import (
	"context"
	"io"
	"queue/model"
	messageServ "queue/service/message"
)

type MessageService interface {
	AppendMessage(context.Context, *model.Message) error
	Poll(context.Context, string) (*model.Message, error)
	AckMessage(context.Context, string, *model.Message) error
	Close(context.Context) error
	Open(context.Context) error
	RecoverFromSnapshot(context.Context, io.Reader) error
	Snapshot(context.Context, io.Writer) error
}

var _ MessageService = (*messageServ.DefaultMessageService)(nil)
