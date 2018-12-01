package pubsub

import (
	"context"
	"encoding/json"
	"os"
	"testing"

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
