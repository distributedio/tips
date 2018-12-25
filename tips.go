package tips

import (
	"context"
	"fmt"

	"github.com/tipsio/tips/store/pubsub"
	"go.uber.org/zap"
)

var (
	// ErrNotFound no found error
	ErrNotFound = "%s can not found"
)

// Tips is an instance structure that contains pubsub
type Tips struct {
	ps *pubsub.Pubsub
}

// PullReq is a collection of pull request information
type PullReq struct {
	SubName string
	Topic   string
	Limit   int64
	AutoACK bool
	Offset  string
}

// Topic is pubsub Topic
type Topic struct {
	pubsub.Topic
}

// Subscription is pubsub Subscription
type Subscription struct {
	pubsub.Subscription
}

// Snapshot is pubsub Snapshot
type Snapshot struct {
	pubsub.Snapshot
}

// Message is a layer of encapsulation of the message
type Message struct {
	Payload []byte
	ID      string
}

// NewTips new a tips object
func NewTips(path string) (tips *Tips, err error) {
	ps, err := pubsub.Open(path)
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}

// MockTips mock a tips object
func MockTips() (*Tips, error) {
	ps, err := pubsub.MockOpen("mocktikv://")
	if err != nil {
		return nil, err
	}
	return &Tips{
		ps: ps,
	}, nil
}

//CreateTopic create a Topic object
func (ti *Tips) CreateTopic(ctx context.Context, topic string) (*Topic, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	defer rollback(txn, err)
	t, err := txn.CreateTopic(topic)
	if err != nil {
		return nil, err
	}
	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	top := &Topic{}
	top.Topic = *t
	return top, nil

}

// Topic returns a topic queried by name
func (ti *Tips) Topic(ctx context.Context, name string) (*Topic, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	defer rollback(txn, err)
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

// Destroy delete a topic
func (ti *Tips) Destroy(ctx context.Context, topic string) error {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	defer rollback(txn, err)
	if err = txn.DeleteTopic(topic); err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// Publish messages and return the allocated message ids for each
// msgids msgids returns the same sequence as the outgoing message
// forbidden topic and MSGS are not empty
func (ti *Tips) Publish(ctx context.Context, msg []string, topic string) ([]string, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	defer rollback(txn, err)

	t, err := txn.GetTopic(topic)

	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}

	if err != nil {
		return nil, err
	}
	message := make([]*pubsub.Message, len(msg))
	for i := range msg {
		message[i] = &pubsub.Message{
			Payload: []byte(msg[i]),
		}
	}

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

// Ack acknowledges a message
func (ti *Tips) Ack(ctx context.Context, msgid string, topic string, subName string) (err error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	defer rollback(txn, err)
	t, err := txn.GetTopic(topic)
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

// Subscribe a topic
func (ti *Tips) Subscribe(ctx context.Context, subName string, topic string) (*Subscription, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	defer rollback(txn, err)

	t, err := txn.GetTopic(topic)
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

// Unsubscribe a topic and subscription
func (ti *Tips) Unsubscribe(ctx context.Context, subName string, topic string) error {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	defer rollback(txn, err)
	//查看当前topic是否存在
	t, err := txn.GetTopic(topic)
	if err == pubsub.ErrNotFound {
		return fmt.Errorf(ErrNotFound, "topic")
	}
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

// Pull messages of a topic from req
// returns the contents of the pull message
func (ti *Tips) Pull(ctx context.Context, req *PullReq) ([]*Message, error) {
	var messages []*Message
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	defer rollback(txn, err)
	t, err := txn.GetTopic(req.Topic)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}

	if err != nil {
		return nil, err
	}
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
	begin := sub.Acked

	if req.Offset != "" {
		begin = pubsub.OffsetFromString(req.Offset)
	}
	if err = txn.Scan(t, begin.Next(), scan); err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return messages, txn.Commit(ctx)
	}

	sub.Sent = pubsub.OffsetFromString(messages[len(messages)-1].ID)
	if req.AutoACK {
		sub.Acked = sub.Sent
	}
	txn.UpdateSubscription(t, sub)

	if err = txn.Commit(ctx); err != nil {
		return nil, err
	}
	return messages, nil
}

// CreateSnapshots creates a snapshot for a subscription
// Return to create snapshots Objcet
func (ti *Tips) CreateSnapshots(ctx context.Context, SnapName string, subName string, topic string) (*Snapshot, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	defer rollback(txn, err)

	t, err := txn.GetTopic(topic)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}
	if err != nil {
		return nil, err
	}

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

// GetSnapshot get a Snapshot
func (ti *Tips) GetSnapshot(ctx context.Context, SnapName string, subName string, topic string) (*Snapshot, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	defer rollback(txn, err)

	t, err := txn.GetTopic(topic)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}

	if err != nil {
		return nil, err
	}

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

// DeleteSnapshots delete a snapshot Object
func (ti *Tips) DeleteSnapshots(ctx context.Context, SnapName string, subName string, topic string) error {
	txn, err := ti.ps.Begin()
	if err != nil {
		return err
	}
	defer rollback(txn, err)

	t, err := txn.GetTopic(topic)
	if err == pubsub.ErrNotFound {
		return fmt.Errorf(ErrNotFound, "topic")
	}
	if err != nil {
		return err
	}
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

// Seek to a Subscription
func (ti *Tips) Seek(ctx context.Context, SnapName string, subName string, topic string) (*Subscription, error) {
	txn, err := ti.ps.Begin()
	if err != nil {
		return nil, err
	}
	defer rollback(txn, err)
	t, err := txn.GetTopic(topic)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "topic")
	}
	if err != nil {
		return nil, err
	}

	sub, err := txn.GetSubscription(t, subName)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "subname")
	}
	if err != nil {
		return nil, err
	}

	snap, err := txn.GetSnapshot(t, sub, SnapName)
	if err == pubsub.ErrNotFound {
		return nil, fmt.Errorf(ErrNotFound, "snapshot")
	}
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

// rollback the transaction
func rollback(txn *pubsub.Transaction, err error) {
	if err != nil {
		if err := txn.Rollback(); err != nil {
			zap.L().Fatal("rollback failed", zap.Error(err))
		}
	}
}
