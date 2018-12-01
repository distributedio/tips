package tips

import (
	"github.com/shafreeck/tips/store/pubsub"
)

func MockTips() (*Tips, error) {
	ps, err := pubsub.MockOpen("mocktikv://")
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}

type PullReq struct {
	SubName string
	Topic   string
	Limit   int64
	Ack     bool
}

type Topic struct {
	pubsub.Topic
}
type Subscription struct {
	pubsub.Subscription
}
type Snapshot struct {
	pubsub.Snapshot
}

type Message struct {
	Payload []byte
	ID      string
}
