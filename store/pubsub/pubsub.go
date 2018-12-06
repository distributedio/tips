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
*  S:{objectid}:{name} // subscription
*  SS:{objectid}:{snapshot}:{name} // snapshot
*  M:{topic}{offset} // message
*
 */

var ErrNotFound = errors.New("not found")

// Offset is the position of a message in a topic
type Offset struct {
	TS    int64 // TS is the StartTS of the transaction
	Index int64
}

// EncodeInt64 encodes an int64 to be memcomparable in asc order
func EncodeInt64(v int64) []byte {
	var buf bytes.Buffer
	if v < 0 {
		v = int64(uint64(v) & 0x7FFFFFFFFFFFFFFF)
	} else if v > 0 {
		v = int64(uint64(v) | 0x8000000000000000)
	}

	// Ignore the error returned here, because buf is a memory io.Writer, can should not fail here
	binary.Write(&buf, binary.BigEndian, v)
	return buf.Bytes()
}

// DecodeInt64 decodes an int64
func DecodeInt64(b []byte) int64 {
	v := int64(binary.BigEndian.Uint64(b))
	if v < 0 {
		v = int64(uint64(v) & 0x7FFFFFFFFFFFFFFF)
	} else if v > 0 {
		v = int64(uint64(v) | 0x8000000000000000)
	}
	return v
}

// Bytes returns offset as bytes
func (offset *Offset) Bytes() []byte {
	var out []byte
	out = append(out, EncodeInt64(offset.TS)...)
	out = append(out, EncodeInt64(offset.Index)...)
	return out
}

// String returns offset as human-friendly string
func (offset *Offset) String() string {
	return fmt.Sprintf("%v-%v", offset.TS, offset.Index)
}

// Next returns a greater offset
func (offset *Offset) Next() *Offset {
	o := *offset
	o.Index++
	return &o
}

// OffsetFromBytes parses offset from bytes
func OffsetFromBytes(d []byte) *Offset {
	ts := DecodeInt64(d[:8])
	idx := DecodeInt64(d[8:])
	return &Offset{TS: ts, Index: idx}
}

// OffsetFromString parses offset from a string
func OffsetFromString(s string) *Offset {
	offset := &Offset{}
	fmt.Sscanf(s, "%d-%d", &offset.TS, &offset.Index)
	return offset
}

// Pubsub is a storage with a pub/sub interface
type Pubsub struct {
	s kv.Storage
}

// Open a pubsub storage
func Open(path string) (*Pubsub, error) {
	s, err := tikv.Driver{}.Open(path)
	if err != nil {
		return nil, err
	}
	return &Pubsub{s: s}, nil
}

// Transaction suppies the api to access pubsub
type Transaction struct {
	t kv.Transaction
}

// Begin a transaction
func (p *Pubsub) Begin() (*Transaction, error) {
	txn, err := p.s.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{t: txn}, nil
}

// Commit a transaction
func (txn *Transaction) Commit(ctx context.Context) error {
	return txn.t.Commit(ctx)
}

// Rollback a transaction
func (txn *Transaction) Rollback() error {
	return txn.t.Rollback()
}

// TopicKey builds a key of a topic
func TopicKey(name string) []byte {
	var key []byte
	key = append(key, 'T', ':')
	key = append(key, []byte(name)...)
	return key
}

// Topic is the meta of a topic
type Topic struct {
	Name      string
	ObjectID  []byte
	CreatedAt int64
}

// UUID generates a global unique ID
func UUID() []byte { return uuid.NewV4().Bytes() }

// CreateTopic creates a topic, if the topic has existed, return it
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

// DeleteTopic deletes a topic
func (txn *Transaction) DeleteTopic(name string) error {
	topic, err := txn.GetTopic(name)
	if err != nil {
		return err
	}
	if err := gc(MessageKey(topic, nil)); err != nil {
		return err
	}
	return txn.t.Delete(TopicKey(name))
}

// GetTopic gets the topic information
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

// SubscriptionKey builds a key of a subscription
func SubscriptionKey(topic *Topic, sub string) []byte {
	var key []byte
	key = append(key, 'S', ':')
	key = append(key, topic.ObjectID...)
	key = append(key, ':')
	key = append(key, []byte(sub)...)
	return key
}

// Subscription keeps the state of subscribers
type Subscription struct {
	Name  string
	Sent  *Offset
	Acked *Offset
}

// CreateSubscritpion creates a subscription
func (txn *Transaction) CreateSubscription(t *Topic, name string) (*Subscription, error) {
	key := SubscriptionKey(t, name)

	val, err := txn.t.Get(key)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		sub := &Subscription{
			Name:  name,
			Sent:  &Offset{int64(txn.t.StartTS()), 0},
			Acked: &Offset{int64(txn.t.StartTS()), 0},
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

// DeleteSubscription deletes a subscription
func (txn *Transaction) DeleteSubscription(t *Topic, name string) error {
	key := SubscriptionKey(t, name)
	return txn.t.Delete(key)
}

// GetSubscription returns a subscription
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

// GetSubscriptions lists all subscriptions of a topic
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

// UpdateSubscription updates a subscription
func (txn *Transaction) UpdateSubscription(t *Topic, s *Subscription) error {
	key := SubscriptionKey(t, s.Name)

	val, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return txn.t.Set(key, val)
}

// MessageID is an identity of message
type MessageID struct {
	*Offset
}

// Message wraps a bytes payload
type Message struct {
	Payload []byte
}

// MessageKey builds a key of a message
func MessageKey(topic *Topic, offset *Offset) []byte {
	var key []byte
	key = append(key, 'M', ':')
	key = append(key, topic.ObjectID...)
	key = append(key, ':')
	if offset != nil {
		key = append(key, offset.Bytes()...)
	}
	return key
}

// Append a message to a topic
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
		mids = append(mids, MessageID{offset})
	}
	return mids, nil
}

// ScanHandler is a handler to process scanned messages
type ScanHandler func(id MessageID, message *Message) bool

// Scan seeks to the offset and calls handler for each message
func (txn *Transaction) Scan(topic *Topic, offset *Offset, handler ScanHandler) error {
	prefix := MessageKey(topic, nil)
	key := MessageKey(topic, offset)
	iter, err := txn.t.Seek(key)
	if err != nil {
		return err
	}

	for iter.Valid() && iter.Key().HasPrefix(prefix) {
		offset := OffsetFromBytes(iter.Key()[len(prefix):])
		msg := &Message{}
		if err := json.Unmarshal(iter.Value(), msg); err != nil {
			return err
		}
		if !handler(MessageID{offset}, msg) {
			break
		}
		if err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot is an immutable state of a subscription
type Snapshot struct {
	Name         string
	Subscription *Subscription
}

// SnapshotKey builds a key of a snapshot
func SnapshotKey(t *Topic, s *Subscription, name string) []byte {
	var key []byte
	key = append(key, 'S', 'S', ':')
	key = append(key, t.ObjectID...)
	key = append(key, ':')
	if s != nil {
		key = append(key, s.Name...)
		key = append(key, ':')
		key = append(key, []byte(name)...)
	}
	return key
}

// CreateSnapshot creates a snapshot for a subscription
func (txn *Transaction) CreateSnapshot(topic *Topic, subscription *Subscription, name string) (*Snapshot, error) {
	key := SnapshotKey(topic, subscription, name)

	val, err := txn.t.Get(key)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		snapshot := &Snapshot{
			Name:         name,
			Subscription: subscription,
		}
		data, err := json.Marshal(snapshot)
		if err != nil {
			return nil, err
		}
		if err := txn.t.Set(key, data); err != nil {
			return nil, err
		}
		return snapshot, nil
	}

	snapshot := &Snapshot{}
	if err := json.Unmarshal(val, snapshot); err != nil {
		return nil, err
	}

	return snapshot, nil
}

// GetSnapshot returns a snapshot
func (txn *Transaction) GetSnapshot(topic *Topic, subscription *Subscription, name string) (*Snapshot, error) {
	key := SnapshotKey(topic, subscription, name)
	val, err := txn.t.Get(key)
	if err != nil {
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		return nil, ErrNotFound
	}

	snapshot := &Snapshot{}
	if err := json.Unmarshal(val, snapshot); err != nil {
		return nil, err
	}

	return snapshot, nil
}

// DeleteSnapshot deletes a snapshot
func (txn *Transaction) DeleteSnapshot(topic *Topic, subscription *Subscription, name string) error {
	return txn.t.Delete(SnapshotKey(topic, subscription, name))
}

// GetSnapshots lists all snapshots of a subscription
func (txn *Transaction) GetSnapshots(topic *Topic, subscription *Subscription) ([]*Snapshot, error) {
	prefix := SnapshotKey(topic, subscription, "")
	iter, err := txn.t.Seek(prefix)
	if err != nil {
		return nil, err
	}

	var snapshots []*Snapshot
	for iter.Valid() && iter.Key().HasPrefix(prefix) {
		ss := &Snapshot{}
		if err := json.Unmarshal(iter.Value(), ss); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, ss)
		if err := iter.Next(); err != nil {
			return nil, err
		}
	}
	return snapshots, nil
}
