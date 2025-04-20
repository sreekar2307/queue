package model

import "fmt"

type Topic struct {
	Name               string
	NumberOfPartitions uint64
}

func (t *Topic) String() string {
	return fmt.Sprintf("Topic{Name: %s, NumberOfPartitions: %d}", t.Name, t.NumberOfPartitions)
}
