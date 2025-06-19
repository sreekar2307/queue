package logger

import (
	"context"
	dragonLogger "github.com/lni/dragonboat/v4/logger"
)

type Attr struct {
	Key   string
	Value any
}

func NewAttr(key string, value any) Attr {
	return Attr{
		Key:   key,
		Value: value,
	}
}

type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

type Logger interface {
	dragonLogger.ILogger
	WithFields(...Attr) Logger
	Debug(context.Context, string, ...Attr)
	Info(context.Context, string, ...Attr)
	Warn(context.Context, string, ...Attr)
	Error(context.Context, string, ...Attr)
	Fatal(context.Context, string, ...Attr)
}
