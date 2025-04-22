package errors

import "errors"

var (
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")

	ErrTopicAlreadyExists         = errors.New("topic already exists")
	ErrConsumerGroupAlreadyExists = errors.New("consumer group already exists")
)
