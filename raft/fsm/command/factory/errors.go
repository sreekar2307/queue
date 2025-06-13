package factory

import (
	stdErrors "errors"
)

var (
	ErrNoCommandsRegistered = stdErrors.New("no commands registered")
	ErrCommandNotFound      = stdErrors.New("command not found")
)
