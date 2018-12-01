package tips

import "context"

type Pubsub interface {
	CreateTopic(cxt context.Context, topic string) (err error)
	Topic(cxt context.Context, topic string) (subName []string, err error)
	Destroy(cxt context.Context, topic string) (err error)

	Publish(cxt context.Context, msg []string, topic string) (msgids []string, err error)
	Ack(cxt context.Context, msgids []string) (err error)

	Subscribe(cxt context.Context, subName string, topic string) (index int64, err error)
	Unsubscribe(cxt context.Context, subName string, topic string) (err error)
	Subscription(cxt context.Context, subName string) (topics string, err error) //topics struct
	Pull(cxt context.Context, subName string, index, limit int64, ack bool) (messages []string, offset int64, err error)

	CreateSnapshots(cxt context.Context, name string, subName string) (index64 int, err error)
	DeleteSnapshots(cxt context.Context, name string, subName string) (err error)
	GetSnapshots(cxt context.Context, subName string) (name string, err error) // name struct
	Seek(cxt context.Context, name string) (index int64, err error)
}
