package consumer_group

import "context"

type ConsumerGroup interface {
	IncrMessageID(context.Context, string, int) error
	LatestMessageID(context.Context, string) (int, error)
}
