package errors

import stdErrors "errors"

var (
	ErrTopicAlreadyExists   = stdErrors.New("topic already exists")
	ErrDuplicateCommand     = stdErrors.New("duplicate command")
	ErrTopicNotFound        = stdErrors.New("topic not found")
	ErrCurrentNodeNotLeader = stdErrors.New("current node is not the leader")
	ErrBrokerNotFound       = stdErrors.New("broker not found")
	ErrBrokerAlreadyExists  = stdErrors.New("broker already exists")
)
