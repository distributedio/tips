package pubsub

import (
	"fmt"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

// Offset 表示一个消息在一个Topic中的位置
type Offset struct {
	Oracle int64 // Oracle 是从PD获取的时间戳
	Index  int64
}

// EncodeInt64 对int64编码，使之在TiKV中升序排列
func EncodeInt64(v int64) []byte {
	var buf bytes.Buffer

	// Ignore the error returned here, because buf is a memory io.Writer, can should not fail here
	binary.Write(&buf, binary.BigEndian, -v)
	return buf.Bytes()
}

// Bytes 返回Offset的二进制形式，可用于内存比较
func (offset *Offset) Bytes() []byte {
	var out []byte
	out = append(out, EncodeInt64(offset.Oracle)...)
	out = append(out, EncodeInt64(offset.Index)...)
	return out
}

// String 返回Offset的字符串格式，便于阅读
func (offset *Offset) String() string {
	return fmt.Sprintf("%v-%v", offset.Oracle, offset.Index)
}

// Pubsub 是一个提供了Pub/Sub原语的存储接口
type Pubsub struct {
}

// Begin 开启一个事务
func (p *Pubsub) Begin() (*Transaction, error) {
}

// Begin 提交一个事务
func (p *Pubsub) Commit() error {
}

// Rollback 回滚一个事务
func (p *Pubsub) Rollback() error {
}

// Transaction 是一个操作Pub/Sub存储的事务
type Transaction struct {
	t kv.Transaction
}

// Topic 是一个保存了Topic信息的对象
type Topic struct {
	name string
}

// CreateTopic 创建一个Topic
func (txn *Transaction) CreateTopic(t *Topic) error {
}

// DeleteTopic 删除一个Topic
func (txn *Transaction) DeleteTopic(name string) error {
}

// GetTopic 获取一个Topic的信息
func (txn *Transaction) GetTopic(name string) (*Topic, error) {
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
}

// DeleteSubscription 删除一个Subscription
func (txn *Transaction) DeleteSubscription(name string) error {
}

// GetSubscription 返回对应Subscription信息
func (txn *Transaction) GetSubscription(name string) (*Subscription, error) {
}
