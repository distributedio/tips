package tips

import (
	"context"

	"github.com/shafreeck/tips/store/pubsub"
)

type Tips struct {
	ps *pubsub.Pubsub
}
type Topic struct {
	pubsub.Topic
}
type Subscription struct {
	pubsub.Subscription
}

func NewTips(path string) (tips *Tips, err error) {
	ps, err := pubsub.Open(path)
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}

//创建一个topic
func (ti *Tips) CreateTopic(cxt context.Context, topic string) (err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	if _, err = txn.CreateTopic(topic); err != nil {
		return err
	}
	if err = txn.Commit(cxt); err != nil {
		return err
	}
	return nil

}

//查看当前topic订阅信息
func (ti *Tips) Topic(ctx context.Context, name string) (topic *Topic, err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	t, err := txn.GetTopic(name)
	if err != nil {
		return nil, err
	}
	topic.CreatedAt = t.CreatedAt
	topic.Name = t.Name
	topic.ObjectID = t.ObjectID

	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return topic, nil
}

//销毁一个topic
func (ti *Tips) Destroy(ctx context.Context, topic string) (err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	if err = txn.DeleteTopic(topic); err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (ti *Tips) Publish(cxt context.Context, msg []string, topic string) (msgids []string, err error) {
}
func (ti *Tips) Ack(cxt context.Context, msgids []string) (err error) {
}

func (ti *Tips) Subscribe(cxt context.Context, subName string, topic string) (index int64, err error) {
}
func (ti *Tips) Unsubscribe(cxt context.Context, subName string, topic string) (err error) {
}
func (ti *Tips) Subscription(cxt context.Context, subName string) (topic string, err error) {

}
func (ti *Tips) Pull(cxt context.Context, subName string, index, limit int64, ack bool) (messages []string, offset int64, err error) {
}

func (ti *Tips) CreateSnapshots(cxt context.Context, name string, subName string) (index64 int, err error) {
}
func (ti *Tips) DeleteSnapshots(cxt context.Context, name string, subName string) (err error) {
}
func (ti *Tips) Seek(cxt context.Context, name string) (index int64, err error) {
}
