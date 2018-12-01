package pubsub

import (
	"github.com/pingcap/tidb/store/mockstore"
)

// MockOpen 打开一个本地内存的Pubsub存储，便于单元测试
func MockOpen(path string) (*Pubsub, error) {
	s, err := mockstore.MockDriver{}.Open(path)
	if err != nil {
		return nil, err
	}
	return &Pubsub{s: s}, nil
}
