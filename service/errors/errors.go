package errors

import stdErrors "errors"

var (
	ErrTopicAlreadyExists = stdErrors.New("topic already exists")
	ErrDuplicateCommand   = stdErrors.New("duplicate command")
	ErrTopicNotFound      = stdErrors.New("topic not found")
)
