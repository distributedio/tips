package pubsub

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

/* Key encoding format
*  T:{name} // topic
*  S:{name} // subscription
*  SS:{name} // snapshot
*  M:{topic}{offset} // message
*
 */

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

// Topic 是一个保存了Topic信息的对象
type Topic struct {
	name string
}

// CreateTopic 创建一个Topic
func (txn *Transaction) CreateTopic(t *Topic) error {
	return nil
}

// DeleteTopic 删除一个Topic
func (txn *Transaction) DeleteTopic(name string) error {
	return nil
}

// GetTopic 获取一个Topic的信息
func (txn *Transaction) GetTopic(name string) (*Topic, error) {
	return nil, nil
}

// Subscription 是一个订阅,保存了当前Topic的相关信息
type Subscription struct {
	name  string
	topic *Topic
	sent  Offset
	acked Offset
}

// CreateSubscritpion 创建一个Subscription
func (txn *Transaction) CreateSubscription(name string, t *Topic) error {
	return nil
}

// DeleteSubscription 删除一个Subscription
func (txn *Transaction) DeleteSubscription(name string) error {
	return nil
}

// GetSubscription 返回对应Subscription信息
func (txn *Transaction) GetSubscription(name string) (*Subscription, error) {
	return nil, nil
}

// Message 代表一个消息对象
type Message struct {
	ID      string
	Payload []byte
}

// Append 将消息添加到Topic
func (txn *Transaction) Append(topic string, messages ...*Message) error {
	prefix := []byte(topic)
	for i := range messages {
		offset := &Offset{TS: int64(txn.t.StartTS()), Index: int64(i)}
		key := append(prefix, offset.Bytes()...)
		data, err := json.Marshal(messages[i])
		if err != nil {
			return err
		}

		if err := txn.t.Set(key, data); err != nil {
			return err
		}
	}
	return nil
}

// ScanHandler 用来处理每一个消息
type ScanHandler func(offset *Offset, message *Message) bool

// Scan 首先跳转到Topic对应的offset，之后将所有消息传递给回调函数
func (txn *Transaction) Scan(topic string, offset *Offset, handler ScanHandler) error {
	key := append([]byte(topic), offset.Bytes()...)
	iter, err := txn.t.Seek(key)
	if err != nil {
		return err
	}
	for iter.Valid() && iter.Key().HasPrefix([]byte(topic)) {
		offset := OffsetFromBytes(iter.Key()[len(topic):])
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
