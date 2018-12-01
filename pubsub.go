package tips

import (
	"context"

	"github.com/shafreeck/tips/store/pubsub"
)

type Pubsub interface {
	CreateTopic(cxt context.Context, topic string) (err error)
	Topic(ctx context.Context, name string) (topic *Topic, err error)
	Destroy(cxt context.Context, topic string) (err error)

	Publish(cxt context.Context, msg []string, topic string) (msgids []string, err error)
	Ack(cxt context.Context, msgids []string) (err error)

	Subscribe(cxt context.Context, subName string, topic string) (index int64, err error)
	Unsubscribe(cxt context.Context, subName string, topic string) (err error)
	Subscription(cxt context.Context, subName string) (topics string, err error) //topics struct
	Pull(cxt context.Context, subName string, index, limit int64, ack bool) (messages []string, offset int64, err error)

	CreateSnapshots(cxt context.Context, name string, subName string) (index64 int, err error)
	DeleteSnapshots(cxt context.Context, name string, subName string) (err error)
	Seek(cxt context.Context, name string) (index int64, err error)
}

func MockPubsub() (Pubsub, error) {
	ps, err := pubsub.MockOpen("mocktikv://")
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}
