package errors

import "errors"

var (
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")

	ErrTopicAlreadyExists    = errors.New("topic already exists")
	ErrConsumerGroupNotFound = errors.New("consumer group not found")
	ErrConsumerNotFound      = errors.New("consumer not found")
)
