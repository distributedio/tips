package tips

import (
	"context"
	"fmt"

	"github.com/shafreeck/tips/store/pubsub"
)

var (
	ErrNotFound = "%s can not found"
)

type Tips struct {
	ps *pubsub.Pubsub
}

func NewTips(path string) (tips Pubsub, err error) {
	ps, err := pubsub.Open(path)
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}

//创建一个topic
func (ti *Tips) CreateTopic(ctx context.Context, topic string) error {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	if _, err = txn.CreateTopic(topic); err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return nil

}

//查看当前topic订阅信息
func (ti *Tips) Topic(ctx context.Context, name string) (*Topic, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(name)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}

	if err != nil {
		return nil, err
	}

	topic := &Topic{Topic: *t}

	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return topic, nil
}

//销毁一个topic
func (ti *Tips) Destroy(ctx context.Context, topic string) error {
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

//Publish 消息下发 支持批量下发,返回下发成功的msgids
//msgids 返回的序列和下发消息序列保持一直
func (ti *Tips) Publish(ctx context.Context, msg []string, topic string) ([]string, error) {
	//获取当前topic
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	//如果当前的topic不存在，那么返回错误
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}

	if err != nil {
		return nil, err
	}
	//将传递进来的msg转化成Append需要的格式
	message := make([]*pubsub.Message, len(msg))
	for i := range msg {
		message[i] = &pubsub.Message{
			Payload: []byte(msg[i]),
		}
	}
	//如果当前的topic存在 则调用Append接口将消息存储到对应的topic下
	// f func(topic *pubsub.Topic, messages ...*pubsub.Message) ([]pubsub.MessageID, error)i
	messageID, err := txn.Append(t, message...)
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	MessageID := make([]string, len(messageID))
	for i := range messageID {
		MessageID[i] = messageID[i].String()
	}

	return MessageID, nil
}

func (ti *Tips) Ack(ctx context.Context, msgids []string) (err error) {
	return nil
}

//Subscribe 创建topic 和 subscription 订阅关系
func (ti *Tips) Subscribe(ctx context.Context, subName string, topic string) (*Subscription, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return nil, err
	}
	//func (txn *Transaction) CreateSubscription(t *Topic, name string) (*Subscription, error)
	s, err := txn.CreateSubscription(t, subName)
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	sub := &Subscription{}
	sub.Subscription = *s
	return sub, nil
}

//Unsubscribe 指定topic 和 subscription 订阅关系
func (ti *Tips) Unsubscribe(ctx context.Context, subName string, topic string) error {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return err
	}
	if err := txn.DeleteSubscription(t, subName); err != nil {
		return err
	}
	return nil
}

//Subscription 查询当前subscription的信息
//func (ti *Tips) Subscription(cxt context.Context, subName string) (string, error) {
//Pull 拉取消息
func (ti *Tips) Pull(ctx context.Context, req *PullReq) ([]Message, error) {
	var messages []Message
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(req.topic)
	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return nil, err
	}
	//获取Subscription
	sub, err := txn.GetSubscription(t, req.subName)
	if err != nil {
		return nil, err
	}
	err = txn.Scan(t, sub.Acked.Next(), func(id pubsub.MessageID, message *pubsub.Message) bool {
		if req.limit <= 0 {
			return false
		}
		messages = append(messages, Message{
			Payload: message.Payload,
			ID:      id.String(),
		})
		req.limit--
		return true

	})
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return messages, nil
}
func (ti *Tips) CreateSnapshots(ctx context.Context, SnapName string, subName string, topic string) (*Snapshot, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return nil, err
	}
	//获取Subscription
	sub, err := txn.GetSubscription(t, subName)
	if err != nil {
		return nil, err
	}
	//f func(topic *pubsub.Topic, subscription *pubsub.Subscription, name string) (*pubsub.Snapshot, error)
	snap, err := txn.CreateSnapshot(t, sub, SnapName)
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	snapshot := &Snapshot{}
	snapshot.Snapshot = *snap
	return snapshot, nil
}
func (ti *Tips) GetSnapshot(ctx context.Context, SnapName string, subName string, topic string) (*Snapshot, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return nil, err
	}
	//获取Subscription
	sub, err := txn.GetSubscription(t, subName)
	if err != nil {
		return nil, err
	}
	snap, err := txn.GetSnapshot(t, sub, SnapName)
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	snapshot := &Snapshot{}
	//	var snapshot []Snapshot
	//	for i := range snap {
	//		snapshot[i].Snapshot = *snap[i]
	//	}
	snapshot.Snapshot = *snap
	return snapshot, nil
}
func (ti *Tips) DeleteSnapshots(ctx context.Context, SnapName string, subName string, topic string) error {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return err
	}
	//获取Subscription
	sub, err := txn.GetSubscription(t, subName)
	if err != nil {
		return err
	}
	err = txn.DeleteSnapshot(t, sub, SnapName)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return nil
}
func (ti *Tips) Seek(ctx context.Context, name string) (int64, error) {
	return 0, nil
}
