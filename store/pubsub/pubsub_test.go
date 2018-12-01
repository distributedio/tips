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
	assert.Equal(t, "S:unittest:sub", string(SubscriptionKey("unittest", "sub")))
	assert.Equal(t, "S:unittest:", string(SubscriptionKey("unittest", "")))
	assert.Equal(t, "S::", string(SubscriptionKey("", "")))
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

		err = txn.t.Set(SubscriptionKey(topic.Name, n), data)
		if err != nil {
			panic(err)
		}
	}
	if err := txn.Commit(context.Background()); err != nil {
		panic(err)
	}
	return subscriptions
}
func CleanupSubscriptions(subscriptions map[string]*Subscription) {
	txn, err := ps.Begin()
	if err != nil {
		panic(err)
	}

	for n := range subscriptions {
		err = txn.t.Delete(SubscriptionKey("unittest", n))
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

	val, err := txn.t.Get(SubscriptionKey("unittest", "sub"))
	assert.NoError(t, err)
	assert.NotNil(t, val)

	got := &Subscription{}
	assert.NoError(t, json.Unmarshal(val, got))

	assert.Equal(t, sub.Name, got.Name)
	assert.Equal(t, "0-0", got.Sent.String())
	assert.Equal(t, "0-0", got.Acked.String())
}

func TestGetSubscription(t *testing.T) {
	topic := &Topic{Name: "unittest", ObjectID: UUID(), CreatedAt: time.Now().UnixNano()}

	txn, err := ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	subscriptions := SetupSubscriptions(topic)
	for n, s := range subscriptions {
		got, err := txn.GetSubscription(topic, n)
		assert.NoError(t, err)
		assert.NotNil(t, got)

		assert.Equal(t, s.Name, got.Name)
		assert.Equal(t, s.Sent.String(), got.Sent.String())
		assert.Equal(t, s.Acked.String(), got.Acked.String())
	}
	assert.NoError(t, txn.Commit(context.Background()))
}
