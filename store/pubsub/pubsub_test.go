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
