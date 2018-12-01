package tips

import (
	"context"

	"github.com/shafreeck/tips/store/pubsub"
)

type Pubsub interface {
	CreateTopic(ctx context.Context, topic string) (err error)
	Topic(ctx context.Context, name string) (topic *Topic, err error)
	Destroy(ctx context.Context, topic string) (err error)

	Publish(ctx context.Context, msg []string, topic string) (msgids []string, err error)
	Ack(ctx context.Context, msgids []string) (err error)

	Subscribe(ctx context.Context, subName string, topic string) (sub *Subscription, err error)
	Unsubscribe(ctx context.Context, subName string, topic string) (err error)
	//Subscription(cxt context.Context, subName string) (topics string, err error) //topics struct
	Pull(ctx context.Context, req *PullReq) (messsages []Message, err error)

	CreateSnapshots(ctx context.Context, SnapName string, subName string, topic string) (snapshot *Snapshot, err error)
	GetSnapshot(ctx context.Context, SnapName string, subName string, topic string) (snapshot *Snapshot, err error)
	DeleteSnapshots(ctx context.Context, Snapname string, subName string, topic string) (err error)
	Seek(ctx context.Context, SnapName string, subName string, topic string) (sub *Subscription, err error)
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

type PullReq struct {
	subName string
	topic   string
	limit   int64
	ack     bool
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
