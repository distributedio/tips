package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicKey(t *testing.T) {
	assert.Equal(t, string(TopicKey("unittest")), "T:unittest")
}
