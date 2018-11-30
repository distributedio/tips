package tipd

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/twinj/uuid"
)

var (
	NameNotFount = "%s cat not found"
)

func GenName() string {
	return uuid.NewV4().String()
}

func ErrNotFound(c *gin.Context, err error, name string) {
	// if err == keyNotFound {
	// c.JSON(http.StatusOK, fmt.Sprintf(NameNotFount, name))
	// }
}

//
func (t *tipd) pull(ctx context.Context, subname string, index, limit int64, ack bool, timeout time.Duration) ([]string, int64, error) {
	tick := time.Tick(timeout)
	var msgs []string
	var offset int64
	for len(msgs) < int(limit) {
		select {
		case <-tick:
			return msgs, offset, nil
		default:
			m, i, err := t.pubsub.Pull(ctx, subname, index, limit, ack)
			if err != nil {
				return nil, 0, err
			}
			msgs = append(msgs, m...)
			offset = i
		}
	}
	return msgs, offset, nil
}
