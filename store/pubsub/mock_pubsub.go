package pubsub

import (
	"github.com/pingcap/tidb/store/mockstore"
)

// MockOpen open a faked pubsub storage
func MockOpen(path string) (*Pubsub, error) {
	s, err := mockstore.MockDriver{}.Open(path)
	if err != nil {
		return nil, err
	}
	return &Pubsub{s: s}, nil
}
