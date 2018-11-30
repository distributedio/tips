package tipd

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

type tipd struct {
	router *gin.Engine
	pubsub Pubsub
	ctx    context.Context
}

func (t *tipd) Serve() {
}

func ok(c *gin.Context, v interface{}) {
	c.JSON(200, v)
}

func (t *tipd) CreateTopic(c *gin.Context) {
	topic := c.Query("topic")
	if len(topic) == 0 {
		c.JSON(http.StatusBadRequest, "create topic is not null")
		return
	}
	if err := t.pubsub.CreateTopic(t.ctx, topic); err != nil {
		c.JSON(http.StatusInternalServerError, "create topic is not null")
		return
	}
	c.Status(http.StatusOK)
	return
}
