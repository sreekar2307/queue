package errors

import "errors"

var (
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")

	ErrTopicAlreadyExists    = errors.New("topic already exists")
	ErrConsumerGroupNotFound = errors.New("consumer group not found")
	ErrConsumerNotFound      = errors.New("consumer not found")

	ErrNoMessageFound = errors.New("messages not found")

	ErrDuplicateCommand = errors.New("duplicate command")

	ErrBrokerNotFound = errors.New("broker not found")
)
