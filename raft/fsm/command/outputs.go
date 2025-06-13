package command

import "github.com/sreekar2307/queue/model"

type (
	CreateTopicOutputs struct {
		Topic   *model.Topic `json:"topic,omitempty"`
		Created bool         `json:"alreadyExists"`
	}
)
