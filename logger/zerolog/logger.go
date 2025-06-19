package zerolog

import (
	"context"
	dragonLogger "github.com/lni/dragonboat/v4/logger"
	"github.com/rs/zerolog"
	"github.com/sreekar2307/queue/logger"
	"os"
)

type Logger struct {
	logger *zerolog.Logger
}

func (l *Logger) SetLevel(dL dragonLogger.LogLevel) {
	var child zerolog.Logger
	switch dL {
	case dragonLogger.DEBUG:
		child = l.logger.Level(zerolog.DebugLevel)
	case dragonLogger.INFO:
		child = l.logger.Level(zerolog.InfoLevel)
	case dragonLogger.WARNING:
		child = l.logger.Level(zerolog.WarnLevel)
	case dragonLogger.ERROR:
		child = l.logger.Level(zerolog.ErrorLevel)
	case dragonLogger.CRITICAL:
		child = l.logger.Level(zerolog.FatalLevel)
	}
	l.logger = &child
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.logf(zerolog.DebugLevel, format, args...)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.logf(zerolog.InfoLevel, format, args...)
}

func (l *Logger) Warningf(format string, args ...interface{}) {
	l.logf(zerolog.WarnLevel, format, args...)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.logf(zerolog.ErrorLevel, format, args...)
}

func (l *Logger) Panicf(format string, args ...interface{}) {
	l.logf(zerolog.PanicLevel, format, args...)
}

func NewLogger(level logger.Level) logger.Logger {
	zerolog.SetGlobalLevel(zerolog.Level(level))
	zl := zerolog.New(os.Stdout).With().Timestamp().Logger()
	return &Logger{logger: &zl}
}

func (l *Logger) Debug(ctx context.Context, s string, attrs ...logger.Attr) {
	l.log(ctx, zerolog.DebugLevel, s, attrs...)
}

func (l *Logger) Info(ctx context.Context, s string, attrs ...logger.Attr) {
	l.log(ctx, zerolog.InfoLevel, s, attrs...)
}

func (l *Logger) Warn(ctx context.Context, s string, attrs ...logger.Attr) {
	l.log(ctx, zerolog.WarnLevel, s, attrs...)
}

func (l *Logger) Error(ctx context.Context, s string, attrs ...logger.Attr) {
	l.log(ctx, zerolog.ErrorLevel, s, attrs...)
}

func (l *Logger) Fatal(ctx context.Context, s string, attrs ...logger.Attr) {
	l.log(ctx, zerolog.FatalLevel, s, attrs...)
}

func (l *Logger) log(_ context.Context, level zerolog.Level, s string, attrs ...logger.Attr) {
	if l.logger.GetLevel() > level {
		return
	}
	event := l.logger.WithLevel(level)
	for _, a := range attrs {
		event = event.Interface(a.Key, a.Value)
	}
	event.Msg(s)
}

func (l *Logger) WithFields(attrs ...logger.Attr) logger.Logger {
	ctx := l.logger.With()
	for _, a := range attrs {
		ctx = ctx.Interface(a.Key, a.Value)
	}
	child := ctx.Logger()
	return &Logger{logger: &child}
}

func (l *Logger) logf(level zerolog.Level, format string, args ...interface{}) {
	if l.logger.GetLevel() > level {
		return
	}
	event := l.logger.WithLevel(level)
	event.Msgf(format, args...)
}
