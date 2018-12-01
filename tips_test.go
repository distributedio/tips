package tips

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTopic(t *testing.T) {
	tips, err := MockTips()
	if err != nil {
		panic(err)
	}
	top, err := tips.CreateTopic(context.Background(), "TestTopic")
	assert.NoError(t, err)

	txn, err := tips.ps.Begin()
	assert.NoError(t, err)

	topic, err := txn.GetTopic("TestTopic")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	assert.Equal(t, topic.Name, top.Name)
	assert.Equal(t, topic.CreatedAt, top.CreatedAt)
	assert.Equal(t, topic.ObjectID, top.ObjectID)
}
func TestTopic(t *testing.T) {
	tips, err := MockTips()
	if err != nil {
		panic(err)
	}
	top1, err := tips.CreateTopic(context.Background(), "t1")
	assert.NoError(t, err)

	t1, err := tips.Topic(context.Background(), "t1")
	assert.NoError(t, err)

	_, err2 := tips.Topic(context.Background(), "t2")

	assert.Equal(t, err2, fmt.Errorf(ErrNotFound, "topic"))

	assert.Equal(t, top1.Name, t1.Name)
	assert.Equal(t, top1.CreatedAt, t1.CreatedAt)
	assert.Equal(t, top1.ObjectID, t1.ObjectID)

}
func TestDestroy(t *testing.T) {
	tips, err := MockTips()
	if err != nil {
		panic(err)
	}
	top1, err := tips.CreateTopic(context.Background(), "t1")
	assert.NoError(t, err)

	t1, err := tips.Topic(context.Background(), "t1")
	assert.NoError(t, err)

	assert.Equal(t, top1.Name, t1.Name)
	assert.Equal(t, top1.CreatedAt, t1.CreatedAt)
	assert.Equal(t, top1.ObjectID, t1.ObjectID)

	err = tips.Destroy(context.Background(), "t1")
	assert.NoError(t, err)
	_, err2 := tips.Topic(context.Background(), "t1")

	assert.Equal(t, err2, fmt.Errorf(ErrNotFound, "topic"))
}
func TestPublish(t *testing.T) {

}
func TestAck(t *testing.T) {

}

func TestSubscribe(t *testing.T) {

}
func TestUnsubscribe(t *testing.T) {

}
func TestPull(t *testing.T) {

}

func TestCreateSnapshots(t *testing.T) {

}
func TestGetSnapshot(t *testing.T) {

}
func TestDeleteSnapshots(t *testing.T) {

}
func TestSeek(t *testing.T) {

}
