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
func (ti *Tips) CreateTopic(cxt context.Context, topic string) error {
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
func (ti *Tips) Topic(ctx context.Context, name string) (*Topic, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	//查看当前topic是否存在
	t, err := txn.GetTopic(name)
	if err != nil {
		return nil, err
	}
	//如果存在则返回topic信息
	topic := &Topic{}

	topic.CreatedAt = t.CreatedAt
	topic.Name = t.Name
	topic.ObjectID = t.ObjectID

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
func (ti *Tips) Publish(cxt context.Context, msg []string, topic string) ([]string, error) {
	//获取当前topic
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
	//将传递进来的msg转化成Append需要的格式
	message := make([]*pubsub.Message, len(msg))
	for i := range msg {
		message[i].Payload = []byte(msg[i])
	}
	//type MessageID struct {
	//    *Offset
	//}
	//messageID = []struct{*Offset}   Offset ==type  type Offset struct {    TS    int64 // TS 是从PD获取的时间  Index int64 }
	//messageID[i]=struct{*Offset}
	//如果当前的topic存在 则调用Append接口将消息存储到对应的topic下
	// f func(topic *pubsub.Topic, messages ...*pubsub.Message) ([]pubsub.MessageID, error)i
	messageID, err := txn.Append(t, message...)
	if err != nil {
		return nil, err
	}
	MessageID := make([]string, len(messageID))
	for i := range messageID {
		MessageID = append(MessageID, messageID[i].String())
	}
	return nil, nil
}

func (ti *Tips) Ack(cxt context.Context, msgids []string) (err error) {
	return nil
}

//Subscribe 指定topic 和 subscription 订阅关系
func (ti *Tips) Subscribe(cxt context.Context, subName string, topic string) (int64, error) {
	return 0, nil
}

//Unsubscribe 指定topic 和 subscription 订阅关系
func (ti *Tips) Unsubscribe(cxt context.Context, subName string, topic string) error {
	return nil
}

//Subscription 查询当前subscription的信息
func (ti *Tips) Subscription(cxt context.Context, subName string) (string, error) {
	return "", nil

}

func (ti *Tips) Pull(cxt context.Context, subName string, index, limit int64, ack bool) ([]string, int64, error) {
	return nil, 0, nil
}

func (ti *Tips) CreateSnapshots(cxt context.Context, name string, subName string) (int, error) {
	return 0, nil
}
func (ti *Tips) DeleteSnapshots(cxt context.Context, name string, subName string) error {
	return nil
}
func (ti *Tips) Seek(cxt context.Context, name string) (int64, error) {
	return 0, nil
}
