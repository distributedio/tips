package pubsub

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/satori/go.uuid"
)

/* Key encoding format
*  T:{name} // topic
*  S:{topic}:{name} // subscription
*  SS:{name} // snapshot
*  M:{topic}{offset} // message
*
 */

var ErrNotFound = errors.New("not found")

// Offset 表示一个消息在一个Topic中的位置
type Offset struct {
	TS    int64 // TS 是从PD获取的时间戳
	Index int64
}

// EncodeInt64 对int64编码，使之在TiKV中升序排列
func EncodeInt64(v int64) []byte {
	var buf bytes.Buffer

	// Ignore the error returned here, because buf is a memory io.Writer, can should not fail here
	binary.Write(&buf, binary.BigEndian, -v)
	return buf.Bytes()
}

// DecodeInt64 从二进制中解码int64
func DecodeInt64(b []byte) int64 {
	v := int64(binary.BigEndian.Uint64(b))
	return -v
}

// Bytes 返回Offset的二进制形式，可用于内存比较
func (offset *Offset) Bytes() []byte {
	var out []byte
	out = append(out, EncodeInt64(offset.TS)...)
	out = append(out, EncodeInt64(offset.Index)...)
	return out
}

// String 返回Offset的字符串格式，便于阅读
func (offset *Offset) String() string {
	return fmt.Sprintf("%v-%v", offset.TS, offset.Index)
}

// Next 返回大于当前Offset的一个Key
func (offset *Offset) Next() []byte {
	return append(offset.Bytes(), 0)
}

// OffsetFromBytes 从二进制数据解析Offset
func OffsetFromBytes(d []byte) *Offset {
	ts := DecodeInt64(d[:8])
	idx := DecodeInt64(d[8:])
	return &Offset{TS: ts, Index: idx}
}

// Pubsub 是一个提供了Pub/Sub原语的存储接口
type Pubsub struct {
	s kv.Storage
}

// Open 打开一个Pubsub存储
func Open(path string) (*Pubsub, error) {
	s, err := tikv.Driver{}.Open(path)
	if err != nil {
		return nil, err
	}
	return &Pubsub{s: s}, nil
}

// Transaction 是一个操作Pub/Sub存储的事务
type Transaction struct {
	t kv.Transaction
}

// Begin 开启一个事务
func (p *Pubsub) Begin() (*Transaction, error) {
	txn, err := p.s.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{t: txn}, nil
}

// Begin 提交一个事务
func (txn *Transaction) Commit(ctx context.Context) error {
	return txn.t.Commit(ctx)
}

// Rollback 回滚一个事务
func (txn *Transaction) Rollback() error {
	return txn.t.Rollback()
}

// TopicKey 根据Topic的name创建Key
func TopicKey(name string) []byte {
	var key []byte
	key = append(key, 'T', ':')
	key = append(key, []byte(name)...)
	return key
}

// Topic 是一个保存了Topic信息的对象
type Topic struct {
	Name      string
	ObjectID  []byte
	CreatedAt int64
}

// UUID 生成全局唯一ID
func UUID() []byte { return uuid.NewV4().Bytes() }

// CreateTopic 创建一个Topic，如果Topic已经存在则返回当前的Topic
func (txn *Transaction) CreateTopic(name string) (*Topic, error) {
	key := TopicKey(name)

	val, err := txn.t.Get(key)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		topic := &Topic{
			Name:      name,
			ObjectID:  UUID(),
			CreatedAt: time.Now().UnixNano(),
		}
		data, err := json.Marshal(topic)
		if err != nil {
			return nil, err
		}
		if err := txn.t.Set(key, data); err != nil {
			return nil, err
		}
		return topic, nil
	}

	topic := &Topic{}
	if err := json.Unmarshal(val, topic); err != nil {
		return nil, err
	}

	return topic, nil
}

// DeleteTopic 删除一个Topic
func (txn *Transaction) DeleteTopic(name string) error {
	topic, err := txn.GetTopic(name)
	if err != nil {
		return err
	}

	return gc(MessageKey(topic, nil))
}

// GetTopic 获取一个Topic的信息
func (txn *Transaction) GetTopic(name string) (*Topic, error) {
	key := TopicKey(name)

	val, err := txn.t.Get(key)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		return nil, ErrNotFound
	}

	topic := &Topic{}
	if err := json.Unmarshal(val, topic); err != nil {
		return nil, err
	}

	return topic, nil
}

func SubscriptionKey(topic *Topic, sub string) []byte {
	var key []byte
	key = append(key, 'S', ':')
	key = append(key, topic.ObjectID...)
	key = append(key, ':')
	key = append(key, []byte(sub)...)
	return key
}

// Subscription 是一个订阅,保存了当前Topic的相关信息
type Subscription struct {
	Name  string
	Sent  Offset
	Acked Offset
}

// CreateSubscritpion 创建一个Subscription
func (txn *Transaction) CreateSubscription(t *Topic, name string) (*Subscription, error) {
	key := SubscriptionKey(t, name)

	val, err := txn.t.Get(key)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		sub := &Subscription{
			Name: name,
		}
		data, err := json.Marshal(sub)
		if err != nil {
			return nil, err
		}
		if err := txn.t.Set(key, data); err != nil {
			return nil, err
		}
		return sub, nil
	}

	sub := &Subscription{}
	if err := json.Unmarshal(val, sub); err != nil {
		return nil, err
	}

	return sub, nil
}

// DeleteSubscription 删除一个Subscription
func (txn *Transaction) DeleteSubscription(t *Topic, name string) error {
	key := SubscriptionKey(t, name)
	return txn.t.Delete(key)
}

// GetSubscription 返回对应Subscription信息
func (txn *Transaction) GetSubscription(t *Topic, name string) (*Subscription, error) {
	key := SubscriptionKey(t, name)

	val, err := txn.t.Get(key)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		return nil, ErrNotFound
	}

	sub := &Subscription{}
	if err := json.Unmarshal(val, sub); err != nil {
		return nil, err
	}

	return sub, nil
}

// GetSubscriptions 返回Topic下所有的订阅关系
func (txn *Transaction) GetSubscriptions(t *Topic) ([]*Subscription, error) {
	var subscriptions []*Subscription

	prefix := SubscriptionKey(t, "")
	iter, err := txn.t.Seek(prefix)
	if err != nil {
		return nil, err
	}

	for iter.Valid() && iter.Key().HasPrefix(prefix) {
		sub := &Subscription{}
		if err := json.Unmarshal(iter.Value(), sub); err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, sub)
		if err := iter.Next(); err != nil {
			return nil, err
		}
	}
	return subscriptions, nil
}

// MessageID 唯一标识一个消息
type MessageID struct {
	*Offset
}

// Message 代表一个消息对象
type Message struct {
	Payload []byte
}

// MessageKey 根据提供的Offset构建一个Key，用来索引消息
func MessageKey(topic *Topic, offset *Offset) []byte {
	var key []byte
	key = append(key, 'M', ':')
	key = append(key, topic.ObjectID...)
	if offset != nil {
		key = append(key, offset.Bytes()...)
	}
	return key
}

// Append 将消息添加到Topic
func (txn *Transaction) Append(topic *Topic, messages ...*Message) ([]MessageID, error) {
	var mids []MessageID
	for i := range messages {
		offset := &Offset{TS: int64(txn.t.StartTS()), Index: int64(i)}
		key := MessageKey(topic, offset)
		data, err := json.Marshal(messages[i])
		if err != nil {
			return nil, err
		}

		if err := txn.t.Set(key, data); err != nil {
			return nil, err
		}
		mids = append(mids, offset)
	}
	return mids, nil
}

// ScanHandler 用来处理每一个消息
type ScanHandler func(id MessageID, message *Message) bool

// Scan 首先跳转到Topic对应的offset，之后将所有消息传递给回调函数
func (txn *Transaction) Scan(topic *Topic, offset *Offset, handler ScanHandler) error {
	key := MessageKey(topic, offset)
	iter, err := txn.t.Seek(key)
	if err != nil {
		return err
	}
	for iter.Valid() && iter.Key().HasPrefix([]byte(topic.Name)) {
		offset := OffsetFromBytes(iter.Key()[len(topic.Name):])
		msg := &Message{}
		if err := json.Unmarshal(iter.Value(), msg); err != nil {
			return err
		}
		if !handler(offset, msg) {
			break
		}
		if err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}
