package pubsub

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var ps *Pubsub

func TestMain(m *testing.M) {
	var err error
	ps, err = MockOpen("mocktikv:///tmp/tips/unittest")
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestTopicKey(t *testing.T) {
	assert.Equal(t, string(TopicKey("unittest")), "T:unittest")
}

func SetupTopics() map[string]*Topic {
	txn, err := ps.Begin()
	if err != nil {
		panic(err)
	}
	topics := map[string]*Topic{
		"t1": &Topic{Name: "t1", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()},
		"t2": &Topic{Name: "t2", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()},
		"t3": &Topic{Name: "t3", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()},
	}
	for n, t := range topics {
		data, err := json.Marshal(t)
		if err != nil {
			panic(err)
		}

		txn.t.Set(TopicKey(n), data)
	}
	err = txn.Commit(context.Background())
	if err != nil {
		panic(err)
	}
	return topics
}

func CleanupTopics(topics map[string]*Topic) {
	txn, err := ps.Begin()
	if err != nil {
		panic(err)
	}
	for n := range topics {
		txn.t.Delete(TopicKey(n))
	}
	err = txn.Commit(context.Background())
	if err != nil {
		panic(err)
	}
}

func TestCreateTopic(t *testing.T) {
	txn, err := ps.Begin()
	assert.NoError(t, err)

	topic, err := txn.CreateTopic("unittest")
	t.Log("topic:", topic)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, err = ps.Begin()
	assert.NoError(t, err)
	val, err := txn.t.Get([]byte("T:unittest"))
	assert.NoError(t, err)

	got := &Topic{}
	err = json.Unmarshal(val, got)
	assert.NoError(t, err)

	assert.Equal(t, topic.Name, got.Name)
	assert.Equal(t, topic.ObjectID, got.ObjectID)
	assert.Equal(t, topic.CreatedAt, got.CreatedAt)
}

func TestGetTopic(t *testing.T) {
	topics := SetupTopics()

	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	for name, topic := range topics {
		got, err := txn.GetTopic(name)
		assert.NoError(t, err)
		assert.Equal(t, topic.Name, got.Name)
		assert.Equal(t, topic.ObjectID, got.ObjectID)
		assert.Equal(t, topic.CreatedAt, got.CreatedAt)
	}

	CleanupTopics(topics)
}

func TestDeleteTopic(t *testing.T) {
	topics := SetupTopics()

	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)
	for name := range topics {
		assert.NoError(t, txn.DeleteTopic(name))
	}
}

func TestSubscriptionKey(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}
	var expected []byte
	expected = append(expected, 'S', ':')
	expected = append(expected, topic.ObjectID...)
	expected = append(expected, []byte(":sub")...)
	assert.Equal(t, expected, SubscriptionKey(topic, "sub"))
}

func SetupSubscriptions(topic *Topic) map[string]*Subscription {
	txn, err := ps.Begin()
	if err != nil {
		panic(err)
	}

	subscriptions := map[string]*Subscription{
		"s1": &Subscription{Name: "s1", Sent: Offset{1, 0}, Acked: Offset{1, 0}},
		"s2": &Subscription{Name: "s2", Sent: Offset{2, 0}, Acked: Offset{2, 0}},
		"s3": &Subscription{Name: "s3", Sent: Offset{3, 0}, Acked: Offset{3, 0}},
	}

	for n, s := range subscriptions {
		data, err := json.Marshal(s)
		if err != nil {
			panic(err)
		}

		err = txn.t.Set(SubscriptionKey(topic, n), data)
		if err != nil {
			panic(err)
		}
	}
	if err := txn.Commit(context.Background()); err != nil {
		panic(err)
	}
	return subscriptions
}
func CleanupSubscriptions(topic *Topic, subscriptions map[string]*Subscription) {
	txn, err := ps.Begin()
	if err != nil {
		panic(err)
	}

	for n := range subscriptions {
		err = txn.t.Delete(SubscriptionKey(topic, n))
		if err != nil {
			panic(err)
		}
	}

	if err := txn.Commit(context.Background()); err != nil {
		panic(err)
	}
}

func TestCreateSubscription(t *testing.T) {
	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	topic := &Topic{
		Name:      "unittest",
		ObjectID:  UUID(),
		CreatedAt: time.Now().UnixNano(),
	}
	sub, err := txn.CreateSubscription(topic, "sub")
	assert.NoError(t, err)
	assert.NotNil(t, sub)

	val, err := txn.t.Get(SubscriptionKey(topic, "sub"))
	assert.NoError(t, err)
	assert.NotNil(t, val)

	got := &Subscription{}
	assert.NoError(t, json.Unmarshal(val, got))

	offset := &Offset{int64(txn.t.StartTS()), 0}
	assert.Equal(t, sub.Name, got.Name)
	assert.Equal(t, offset.String(), got.Sent.String())
	assert.Equal(t, offset.String(), got.Acked.String())
}

func TestGetSubscription(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}

	subscriptions := SetupSubscriptions(topic)

	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	for n, s := range subscriptions {
		got, err := txn.GetSubscription(topic, n)
		assert.NoError(t, err)
		assert.NotNil(t, got)

		assert.Equal(t, s.Name, got.Name)
		assert.Equal(t, s.Sent.String(), got.Sent.String())
		assert.Equal(t, s.Acked.String(), got.Acked.String())
	}
	assert.NoError(t, txn.Commit(context.Background()))

	CleanupSubscriptions(topic, subscriptions)
}

func TestDeleteSubscription(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}
	subscriptions := SetupSubscriptions(topic)

	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	for n := range subscriptions {
		t.Log(string(SubscriptionKey(topic, n)))
		err := txn.DeleteSubscription(topic, n)
		assert.NoError(t, err)
	}
	assert.NoError(t, txn.Commit(context.Background()))

	// 检查是否真的删除
	txn, err = ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)
	for n := range subscriptions {
		got, err := txn.GetSubscription(topic, n)
		assert.Equal(t, ErrNotFound, err)
		assert.Nil(t, got)
	}
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestGetSubscriptions(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}
	subscriptions := SetupSubscriptions(topic)
	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	subs, err := txn.GetSubscriptions(topic)
	assert.NoError(t, err)
	assert.NotNil(t, subs)

	assert.Equal(t, len(subscriptions), len(subs))
	for _, got := range subs {
		s := subscriptions[got.Name]
		assert.Equal(t, s.Sent.String(), got.Sent.String())
		assert.Equal(t, s.Acked.String(), got.Acked.String())
	}

	CleanupSubscriptions(topic, subscriptions)
}

func TestUpdateSubscription(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}
	subscriptions := SetupSubscriptions(topic)

	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	sentOffsets := make([]Offset, len(subscriptions))
	ackedOffsets := make([]Offset, len(subscriptions))
	for i := range sentOffsets {
		sentOffsets[i] = Offset{int64(i), int64(i)}
		ackedOffsets[i] = Offset{int64(i), int64(i)}
	}

	i := 0
	for _, s := range subscriptions {
		s.Sent = sentOffsets[i]
		s.Acked = sentOffsets[i]
		i++

		assert.NoError(t, txn.UpdateSubscription(topic, s))
	}
	subs, err := txn.GetSubscriptions(topic)
	assert.NoError(t, err)
	assert.NotNil(t, subs)

	assert.Equal(t, len(subscriptions), len(subs))
	for _, got := range subs {
		s := subscriptions[got.Name]
		assert.Equal(t, s.Sent.String(), got.Sent.String())
		assert.Equal(t, s.Acked.String(), got.Acked.String())
	}

	CleanupSubscriptions(topic, subscriptions)
}

func TestSnapshotKey(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}
	subscription := &Subscription{Name: "sub"}
	var expected []byte
	expected = append(expected, 'S', 'S', ':')
	expected = append(expected, topic.ObjectID...)
	expected = append(expected, ':')
	expected = append(expected, []byte(subscription.Name)...)
	expected = append(expected, []byte(":snap")...)
	assert.Equal(t, expected, SnapshotKey(topic, subscription, "snap"))

	var prefix []byte
	prefix = append(prefix, 'S', 'S', ':')
	prefix = append(prefix, topic.ObjectID...)
	prefix = append(prefix, ':')
	assert.Equal(t, prefix, SnapshotKey(topic, nil, ""))
}

func TestCreateSnapshot(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}
	subscription := &Subscription{Name: "sub", Sent: Offset{time.Now().UnixNano(), 0}, Acked: Offset{time.Now().UnixNano(), 0}}

	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	snapshot, err := txn.CreateSnapshot(topic, subscription, "snap")
	assert.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.NotNil(t, snapshot.Subscription)

	val, err := txn.t.Get(SnapshotKey(topic, subscription, "snap"))
	assert.NoError(t, err)
	assert.NotNil(t, val)

	got := &Snapshot{}
	assert.NoError(t, json.Unmarshal(val, got))

	assert.Equal(t, snapshot.Subscription.Name, got.Subscription.Name)
	assert.Equal(t, snapshot.Subscription.Sent.String(), got.Subscription.Sent.String())
	assert.Equal(t, snapshot.Subscription.Acked.String(), got.Subscription.Acked.String())

	// 当Snapshot已经存在时，返回存在的Snapshot
	subscription2 := &Subscription{Name: "sub", Sent: Offset{time.Now().UnixNano(), 0}, Acked: Offset{time.Now().UnixNano(), 0}}
	snapshot, err = txn.CreateSnapshot(topic, subscription2, "snap")
	assert.NoError(t, err)
	assert.NotNil(t, snapshot)

	val, err = txn.t.Get(SnapshotKey(topic, subscription, "snap"))
	assert.NoError(t, err)
	assert.NotNil(t, val)

	got = &Snapshot{}
	assert.NoError(t, json.Unmarshal(val, got))

	assert.Equal(t, snapshot.Subscription.Name, got.Subscription.Name)
	assert.Equal(t, snapshot.Subscription.Sent.String(), got.Subscription.Sent.String())
	assert.Equal(t, snapshot.Subscription.Acked.String(), got.Subscription.Acked.String())

}

func SetupSnapshots(t *Topic, s *Subscription) map[string]*Snapshot {
	now := time.Now().UnixNano()
	snapshots := map[string]*Snapshot{
		"snap1": &Snapshot{&Subscription{Name: "s1", Sent: Offset{now, 0}, Acked: Offset{now, 0}}},
		"snap2": &Snapshot{&Subscription{Name: "s2", Sent: Offset{now + 1, 1}, Acked: Offset{now + 1, 1}}},
		"snap3": &Snapshot{&Subscription{Name: "s3", Sent: Offset{now + 2, 2}, Acked: Offset{now + 2, 2}}},
	}
	txn, err := ps.Begin()
	if err != nil {
		panic(err)
	}
	for n, ss := range snapshots {
		data, err := json.Marshal(ss)
		if err != nil {
			panic(err)
		}

		if err := txn.t.Set(SnapshotKey(t, s, n), data); err != nil {
			panic(err)
		}
	}
	if err := txn.Commit(context.Background()); err != nil {
		panic(err)
	}
	return snapshots
}

func CleanupSnapshots(t *Topic, s *Subscription, snapshots map[string]*Snapshot) {
	txn, err := ps.Begin()
	if err != nil {
		panic(err)
	}
	for n := range snapshots {
		if err := txn.t.Delete(SnapshotKey(t, s, n)); err != nil {
			panic(err)
		}
	}
	if err := txn.Commit(context.Background()); err != nil {
		panic(err)
	}
}

func TestGetSnapshot(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}
	subscription := &Subscription{Name: "sub", Sent: Offset{time.Now().UnixNano(), 0}, Acked: Offset{time.Now().UnixNano(), 0}}

	snapshots := SetupSnapshots(topic, subscription)

	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	for n, ss := range snapshots {
		got, err := txn.GetSnapshot(topic, subscription, n)
		assert.NoError(t, err)
		assert.NotNil(t, got)

		assert.Equal(t, ss.Subscription.Name, got.Subscription.Name)
		assert.Equal(t, ss.Subscription.Sent.String(), got.Subscription.Sent.String())
		assert.Equal(t, ss.Subscription.Acked.String(), got.Subscription.Acked.String())
	}
	assert.NoError(t, txn.Commit(context.Background()))
}
