package storage

import (
	"context"
	"queue/message"
)

type Storage interface {
	WriteMessage(context.Context, *message.Message) (*message.Message, error)
	ReadMessage(context.Context, int) (*message.Message, error)
}
