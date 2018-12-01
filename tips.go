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

type PullReq struct {
	SubName string
	Topic   string
	Limit   int64
	OffAck  bool
	Offset  string
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

func NewTips(path string) (tips *Tips, err error) {
	ps, err := pubsub.Open(path)
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}

func MockTips() (*Tips, error) {
	ps, err := pubsub.MockOpen("mocktikv://")
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

	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return &Topic{Topic: *t}, nil
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

func (ti *Tips) Ack(ctx context.Context, msgid string, topic string, subName string) (err error) {
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
	s, err := txn.GetSubscription(t, subName)
	if err != nil {
		return err
	}
	s.Acked = pubsub.OffsetFromString(msgid)
	err = txn.UpdateSubscription(t, s)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
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
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}

	if err != nil {
		return nil, err
	}

	s, err := txn.CreateSubscription(t, subName)
	if err != nil {
		return nil, err
	}

	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return &Subscription{Subscription: *s}, nil
}

//Unsubscribe 指定topic 和 subscription 订阅关系
func (ti *Tips) Unsubscribe(ctx context.Context, subName string, topic string) error {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	if err == pubsub.ErrNotFound {
		return fmt.Errorf(ErrNotFound, "topic")
	}
	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return err
	}

	if err := txn.DeleteSubscription(t, subName); err != nil {
		return err
	}

	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return nil
}

//Subscription 查询当前subscription的信息
//func (ti *Tips) Subscription(cxt context.Context, subName string) (string, error) {
//Pull 拉取消息
func (ti *Tips) Pull(ctx context.Context, req *PullReq) ([]*Message, error) {
	var messages []*Message
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(req.Topic)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}

	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return nil, err
	}
	//获取Subscription
	sub, err := txn.GetSubscription(t, req.SubName)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "subname")
	}

	if err != nil {
		return nil, err
	}

	scan := func(id pubsub.MessageID, message *pubsub.Message) bool {
		if req.Limit <= 0 {
			return false
		}
		messages = append(messages, &Message{
			Payload: message.Payload,
			ID:      id.String(),
		})
		req.Limit--
		return true
	}

	if req.OffAck {
		sub.Acked = pubsub.OffsetFromString(req.Offset)
	}
	if err = txn.Scan(t, sub.Acked.Next(), scan); err != nil {
		return nil, err
	}

	if !req.OffAck {
		sub.Acked = pubsub.OffsetFromString(messages[len(messages)-1].ID)
	}
	sub.Sent = pubsub.OffsetFromString(messages[len(messages)-1].ID)
	txn.UpdateSubscription(t, sub)

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
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}
	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return nil, err
	}
	//获取Subscription
	sub, err := txn.GetSubscription(t, subName)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "subname")
	}
	if err != nil {
		return nil, err
	}
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
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}

	//如果当前的topic不存在，那么返回错误
	if err != nil {
		return nil, err
	}
	//获取Subscription
	sub, err := txn.GetSubscription(t, subName)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "subname")
	}
	if err != nil {
		return nil, err
	}
	snap, err := txn.GetSnapshot(t, sub, SnapName)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "snap")
	}
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return &Snapshot{Snapshot: *snap}, nil
}

func (ti *Tips) DeleteSnapshots(ctx context.Context, SnapName string, subName string, topic string) error {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	//如果当前的topic不存在，那么返回错误
	if err == pubsub.ErrNotFound {
		return fmt.Errorf(ErrNotFound, "topic")
	}
	if err != nil {
		return err
	}
	//获取Subscription
	sub, err := txn.GetSubscription(t, subName)
	if err == pubsub.ErrNotFound {
		return fmt.Errorf(ErrNotFound, "subname")
	}
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

func (ti *Tips) Seek(ctx context.Context, SnapName string, subName string, topic string) (*Subscription, error) {
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

	//获取snapshot
	snap, err := txn.GetSnapshot(t, sub, SnapName)
	if err != nil {
		return nil, err
	}
	sub.Acked = snap.Subscription.Acked
	sub.Sent = snap.Subscription.Sent

	err = txn.UpdateSubscription(t, sub)
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	subscription := &Subscription{}
	subscription.Subscription = *sub
	return subscription, nil
}
