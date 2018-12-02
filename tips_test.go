package tips

import (
	"context"
	"errors"
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

	tips, err := MockTips()
	if err != nil {
		panic(err)
	}

	//创建topic,测试topic存在的情况
	top1, err := tips.CreateTopic(context.Background(), "t1")
	assert.NoError(t, err)
	assert.NotNil(t, top1)

	sub1, err := tips.Subscribe(context.Background(), "subName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, sub1)

	txn, err := tips.ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	val, err := txn.GetSubscription(&top1.Topic, "subName")
	assert.NoError(t, err)
	assert.NotNil(t, val)
	txn.Commit(context.TODO())

	assert.Equal(t, sub1.Name, val.Name)
	assert.Equal(t, sub1.Sent.String(), val.Sent.String())
	assert.Equal(t, sub1.Acked.String(), val.Acked.String())

	sub2, err := tips.Subscribe(context.Background(), "subName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, sub2)

	assert.Equal(t, sub2.Name, val.Name)
	assert.Equal(t, sub2.Sent.String(), val.Sent.String())
	assert.Equal(t, sub2.Acked.String(), val.Acked.String())
	//测试sub已经存在的情况
	//测试topic不存在的情况
	_, err2 := tips.Subscribe(context.Background(), "subName", "t2")

	assert.Equal(t, err2, fmt.Errorf(ErrNotFound, "topic"))

}
func TestUnsubscribe(t *testing.T) {

	tips, err := MockTips()
	if err != nil {
		panic(err)
	}

	//创建topic,测试topic存在的情况
	top1, err := tips.CreateTopic(context.Background(), "t1")
	assert.NoError(t, err)
	assert.NotNil(t, top1)

	sub1, err := tips.Subscribe(context.Background(), "subName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, sub1)

	txn, err := tips.ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	val, err := txn.GetSubscription(&top1.Topic, "subName")
	assert.NoError(t, err)
	assert.NotNil(t, val)
	txn.Commit(context.TODO())

	assert.Equal(t, sub1.Name, val.Name)
	assert.Equal(t, sub1.Sent.String(), val.Sent.String())
	assert.Equal(t, sub1.Acked.String(), val.Acked.String())

	err = tips.Unsubscribe(context.Background(), "subName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, sub1)

	txn, err = tips.ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	val, err2 := txn.GetSubscription(&top1.Topic, "subName")
	assert.Equal(t, errors.New("not found"), err2)
	assert.Nil(t, val)
	txn.Commit(context.TODO())

}
func TestPull(t *testing.T) {

}

func TestCreateSnapshots(t *testing.T) {
	tips, err := MockTips()
	if err != nil {
		panic(err)
	}

	//创建topic,测试topic存在的情况
	top1, err := tips.CreateTopic(context.Background(), "t1")
	assert.NoError(t, err)
	assert.NotNil(t, top1)

	sub1, err := tips.Subscribe(context.Background(), "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, sub1)

	snap, err := tips.CreateSnapshots(context.Background(), "snapName", "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, snap)

	txn, err := tips.ps.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	sub, err := txn.GetSubscription(&top1.Topic, "SubName")
	assert.NoError(t, err)
	assert.NotNil(t, sub)

	got, err := txn.GetSnapshot(&top1.Topic, sub, "snapName")
	assert.NoError(t, err)
	assert.NotNil(t, sub)

	txn.Commit(context.TODO())
	assert.Equal(t, snap.Name, got.Name)
	assert.Equal(t, snap.Subscription.Name, got.Subscription.Name)
	assert.Equal(t, snap.Subscription.Sent.String(), got.Subscription.Sent.String())
	assert.Equal(t, snap.Subscription.Acked.String(), got.Subscription.Acked.String())

	//当snapshot已经存在时 返回存在的Snapshot
	snap2, err := tips.CreateSnapshots(context.Background(), "snapName", "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, snap.Name, snap2.Name)
	assert.Equal(t, snap.Subscription.Name, snap2.Subscription.Name)
	assert.Equal(t, snap.Subscription.Sent.String(), snap2.Subscription.Sent.String())
	assert.Equal(t, snap.Subscription.Acked.String(), snap2.Subscription.Acked.String())

	//topic  subscription 不存在
	snap2, err2 := tips.CreateSnapshots(context.Background(), "snapName", "SubName", "t2")
	assert.Equal(t, fmt.Errorf(ErrNotFound, "topic"), err2)
	assert.Nil(t, snap2)
	//topic 存在，s 不存在
	top3, err := tips.CreateTopic(context.Background(), "t3")
	assert.NoError(t, err)
	assert.NotNil(t, top3)
	snap3, err3 := tips.CreateSnapshots(context.Background(), "snapName", "SubName", "t3")
	assert.Equal(t, fmt.Errorf(ErrNotFound, "subname"), err3)
	assert.Nil(t, snap3)

}
func TestGetSnapshot(t *testing.T) {
	tips, err := MockTips()
	if err != nil {
		panic(err)
	}

	//创建topic,测试topic存在的情况
	top1, err := tips.CreateTopic(context.Background(), "t1")
	assert.NoError(t, err)
	assert.NotNil(t, top1)

	sub1, err := tips.Subscribe(context.Background(), "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, sub1)

	snap, err := tips.CreateSnapshots(context.Background(), "snapName", "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, snap)

	get, err := tips.GetSnapshot(context.Background(), "snapName", "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, get)

	assert.Equal(t, snap.Name, get.Name)
	assert.Equal(t, snap.Subscription.Name, get.Subscription.Name)
	assert.Equal(t, snap.Subscription.Sent.String(), get.Subscription.Sent.String())
	assert.Equal(t, snap.Subscription.Acked.String(), get.Subscription.Acked.String())
	//测试topic不存在的情况
	get, err = tips.GetSnapshot(context.Background(), "snapName", "SubName", "t2")
	assert.Equal(t, err, fmt.Errorf(ErrNotFound, "topic"))
	assert.Nil(t, get)
	//测试sub不存在的情况
	get, err = tips.GetSnapshot(context.Background(), "snapName", "subName", "t1")
	assert.Equal(t, err, fmt.Errorf(ErrNotFound, "subname"))
	assert.Nil(t, get)
	//测试snapshot不存在的情况
	get, err = tips.GetSnapshot(context.Background(), "SnapName", "SubName", "t1")
	assert.Equal(t, err, fmt.Errorf(ErrNotFound, "snap"))
	assert.Nil(t, get)

}
func TestDeleteSnapshots(t *testing.T) {
	tips, err := MockTips()
	if err != nil {
		panic(err)
	}

	//创建topic,测试topic存在的情况
	top1, err := tips.CreateTopic(context.Background(), "t1")
	assert.NoError(t, err)
	assert.NotNil(t, top1)

	sub1, err := tips.Subscribe(context.Background(), "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, sub1)

	snap, err := tips.CreateSnapshots(context.Background(), "snapName", "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, snap)

	get, err := tips.GetSnapshot(context.Background(), "snapName", "SubName", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, get)

	assert.Equal(t, snap.Name, get.Name)
	assert.Equal(t, snap.Subscription.Name, get.Subscription.Name)
	assert.Equal(t, snap.Subscription.Sent.String(), get.Subscription.Sent.String())
	assert.Equal(t, snap.Subscription.Acked.String(), get.Subscription.Acked.String())

	err = tips.DeleteSnapshots(context.Background(), "snapName", "SubName", "t1")
	assert.NoError(t, err)

	get, err = tips.GetSnapshot(context.Background(), "snapName", "SubName", "t1")
	assert.Equal(t, err, fmt.Errorf(ErrNotFound, "snap"))
	assert.Nil(t, get)

	top2, err := tips.CreateTopic(context.Background(), "t2")
	assert.NoError(t, err)
	assert.NotNil(t, top2)

	sub2, err := tips.Subscribe(context.Background(), "SubName", "t2")
	assert.NoError(t, err)
	assert.NotNil(t, sub2)

	snap2, err := tips.CreateSnapshots(context.Background(), "snapName", "SubName", "t2")
	assert.NoError(t, err)
	assert.NotNil(t, snap2)
	//测试topic不存在的情况
	err = tips.DeleteSnapshots(context.Background(), "snapName", "SubName", "t3")
	assert.Equal(t, err, fmt.Errorf(ErrNotFound, "topic"))
	//测试sub不存在的情况
	err = tips.DeleteSnapshots(context.Background(), "snapName", "subName", "t2")
	assert.Equal(t, err, fmt.Errorf(ErrNotFound, "subname"))
	//测试snapshot不存在的情况
	err = tips.DeleteSnapshots(context.Background(), "SnapName", "SubName", "t2")
	assert.Equal(t, err, nil)

}
func TestSeek(t *testing.T) {

}
