package errors

import stdErrors "errors"

var (
	ErrTopicAlreadyExists   = stdErrors.New("topic already exists")
	ErrTopicNotFound        = stdErrors.New("topic not found")
	ErrCurrentNodeNotLeader = stdErrors.New("current node is not the leader")
	ErrBrokerAlreadyExists  = stdErrors.New("broker already exists")
)
