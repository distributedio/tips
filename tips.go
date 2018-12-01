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
<<<<<<< HEAD
	ps, err := pubsub.Open(path)
=======
	ps, err = NewPubsub(path)
>>>>>>> 849b0838ead2f3d9c787548f556c5570c3dc1acb
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}

//创建一个topic
func (ti *Tips) CreateTopic(cxt context.Context, topic string) (err error) {
	if txn, err := ti.ps.Begin(); err != nil {
		return err
	}
	if err = txn.CreateTopic(topic); err != nil {
		return err
	}
	if err = txn.Commit(cxt); err != nil {
		return err
	}
	return nil

}

//查询当前topic的信息
func (ti *Tips) GetTopic(ctx context.Context) (topic *Topic, err error) {
	if txn, err := ti.ps.Begin(); err != nil {
		return nil, err
	}
	if topic, err := txn.GetTopic(topic); err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return topic, nil
}

//查看当前topic订阅信息
func (ti *Tips) Topic(cxt context.Context, topic string) (topic *Topic, err error) {
	if txn, err := ti.ps.Begin(); err != nil {
		return nil, err
	}
	topic, err := txn.GetTopic(topic)
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return topic, nil

}

//销毁一个topic
func (ti *Tips) Destroy(cxt context.Context, topic string) (err error) {
	if txn, err := ti.ps.Begin(); err != nil {
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
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
}
func (ti *Tips) Ack(cxt context.Context, msgids []string) (err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
}

func (ti *Tips) Subscribe(cxt context.Context, subName string, topic string) (index int64, err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
}
func (ti *Tips) Unsubscribe(cxt context.Context, subName string, topic string) (err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
}
func (ti *Tips) Subscription(cxt context.Context, subName string) (topic string, err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}

}
func (ti *Tips) Pull(cxt context.Context, subName string, index, limit int64, ack bool) (messages []string, offset int64, err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
}

func (ti *Tips) CreateSnapshots(cxt context.Context, name string, subName string) (index64 int, err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
}
func (ti *Tips) DeleteSnapshots(cxt context.Context, name string, subName string) (err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
}
func (ti *Tips) Seek(cxt context.Context, name string) (index int64, err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
}
