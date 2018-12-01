package tips

import (
	"context"

	"github.com/shafreeck/tips/store/pubsub"
)

type Tips struct {
	ps *Pubsub
}

func New(path string) (tips *Tips, err error) {
	ps, err = NewPubsub(path)
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}
func NewPubsub(path string) (*Pubsub, error) {
	ps, err := pubsub.Open(path)
	if err != nil {
		return nil, err
	}
	return ps, nil
}
func (ti *Tips) CreateTopic(cxt context.Context, topic string) (err error) {
	//构造topic对象
	t := &Topic{
		name: topic,
	}
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	err := txn.CreateTopic(t)
	if err != nil {
		return err
	}
	if err := txn.Commit(cxt); err != nil {
		return err
	}
	return nil

}

func (ti *Tips) Topic(cxt context.Context, topic string) (subName []string, err error) {

}
func (ti *Tips) Destroy(cxt context.Context, topic string) (err error) {

}

func (ti *Tips) Publish(cxt context.Context, msg []string, topic string) (msgids []string, err error) {
}
func (ti *Tips) Ack(cxt context.Context, msgids []string) (err error) {

}

func (ti *Tips) Subscribe(cxt context.Context, subName string, topic string) (index int64, err error) {
}
func (ti *Tips) Unsubscribe(cxt context.Context, subName string, topic string) (err error) {

}
func (ti *Tips) Subscription(cxt context.Context, subName string) (topics string, err error) //topics struct{}
func (ti *Tips) Pull(cxt context.Context, subName string, index, limit int64, ack bool) (messages []string, offset int64, err error) {
}

func (ti *Tips) CreateSnapshots(cxt context.Context, name string, subName string) (index64 int, err error) {
}
func (ti *Tips) DeleteSnapshots(cxt context.Context, name string, subName string) (err error) {

}
func (ti *Tips) GetSnapshots(cxt context.Context, subName string) (name string, err error) {

} // name struct{}
func (ti *Tips) Seek(cxt context.Context, name string) (index int64, err error) {

}
