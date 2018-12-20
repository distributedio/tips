package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tipsio/tips"
	"github.com/tipsio/tips/metrics"
)

// CreateTopic creates a topic that returns the client topic information
func (s *Server) CreateTopic(c *gin.Context) {
	start := time.Now()
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	t, err := s.pubsub.CreateTopic(ctx, topic)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, t)
	metrics.GetMetrics().TopicsHistogramVec.WithLabelValues("create").Observe(time.Since(start).Seconds())
	return
}

// Topic returns a topic queried by name
func (t *Server) Topic(c *gin.Context) {
	start := time.Now()
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	msg, err := t.pubsub.Topic(ctx, topic)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, msg)
	metrics.GetMetrics().TopicsHistogramVec.WithLabelValues("topic").Observe(time.Since(start).Seconds())
}

// Destroy deletes a topic
func (t *Server) Destroy(c *gin.Context) {
	start := time.Now()
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	if err := t.pubsub.Destroy(ctx, topic); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.Status(http.StatusOK)
	metrics.GetMetrics().TopicsHistogramVec.WithLabelValues("delete").Observe(time.Since(start).Seconds())
}

// Publish messages and return the allocated message ids for each
// msgids msgids returns the same sequence as the outgoing message
// forbidden topic and MSGS are not empty
func (t *Server) Publish(c *gin.Context) {
	start := time.Now()
	topic := c.Param("topic")
	pub := &struct {
		Messages []string
	}{}
	if err := c.BindJSON(pub); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if len(pub.Messages) == 0 {
		fail(c, http.StatusBadRequest, errors.New("msg is not null"))
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	msgids, err := t.pubsub.Publish(ctx, pub.Messages, topic)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, msgids)
	metrics.GetMetrics().MessagesHistogramVec.WithLabelValues("publish").Observe(time.Since(start).Seconds())

	var size float64
	for _, msg := range pub.Messages {
		size += float64(len(msg))
	}
	metrics.GetMetrics().MessagesSizeHistogramVec.WithLabelValues("publish").Observe(size)
}

// Ack acknowledges a message
func (t *Server) Ack(c *gin.Context) {
	start := time.Now()
	subName := c.Param("subname")
	topic := c.Param("topic")
	msgid := c.Param("msgid")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	err := t.pubsub.Ack(ctx, msgid, topic, subName)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.Status(http.StatusOK)
	metrics.GetMetrics().MessagesHistogramVec.WithLabelValues("ack").Observe(time.Since(start).Seconds())
}

// Subscribe a topic
func (t *Server) Subscribe(c *gin.Context) {
	start := time.Now()
	subName := c.Param("subname")
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	index, err := t.pubsub.Subscribe(ctx, subName, topic)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, index)
	metrics.GetMetrics().SubscribtionsHistogramVec.WithLabelValues("sub").Observe(time.Since(start).Seconds())
}

// Unsubscribe a topic and subscription
func (t *Server) Unsubscribe(c *gin.Context) {
	start := time.Now()
	subName := c.Param("subname")
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	err := t.pubsub.Unsubscribe(ctx, subName, topic)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.Status(http.StatusOK)
	metrics.GetMetrics().SubscribtionsHistogramVec.WithLabelValues("unsub").Observe(time.Since(start).Seconds())
}

// Pull messages of a topic from a given offset
// forbid topic subName to be empty and limit must be greater than 0
// the default timeout is 1s and the timeout unit is s
// returns the position to be pulled next time
func (t *Server) Pull(c *gin.Context) {
	start := time.Now()
	req := &struct {
		Limit   int64
		Timeout int64
		AutoACK bool
		Offset  string
	}{}
	if err := c.BindJSON(req); err != nil && err != io.EOF {
		c.JSON(http.StatusBadRequest, err.Error())
		return
	}
	if req.Limit <= 0 {
		req.Limit = 256
	}
	if req.Timeout == 0 {
		req.Timeout = 3600
	}

	t1 := time.Duration(req.Timeout) * time.Second
	pReq := &tips.PullReq{
		SubName: c.Param("subname"),
		Topic:   c.Param("topic"),
		Limit:   req.Limit,
		AutoACK: req.AutoACK,
		Offset:  req.Offset,
	}
	ctx, cancel := context.WithTimeout(t.ctx, t1)
	defer cancel()
	msgs, err := t.pull(ctx, pReq, t1)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, msgs)
	metrics.GetMetrics().SubscribtionsHistogramVec.WithLabelValues("pull").Observe(time.Since(start).Seconds())
	var size float64
	for _, msg := range msgs {
		size += float64(len(msg.Payload))
		size += float64(len(msg.ID))
	}
	metrics.GetMetrics().MessagesSizeHistogramVec.WithLabelValues("pull").Observe(size)
}

// CreateSnapshots creates a snapshot for a subscription
// Return to create snapshots message
func (t *Server) CreateSnapshots(c *gin.Context) {
	start := time.Now()
	subName := c.Param("subname")
	name := c.Param("name")
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	_, err := t.pubsub.CreateSnapshots(ctx, name, subName, topic)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, name)
	metrics.GetMetrics().SnapshotsHistogramVec.WithLabelValues("create").Observe(time.Since(start).Seconds())
}

// DeleteSnapshots delete a snapshot
func (t *Server) DeleteSnapshots(c *gin.Context) {
	start := time.Now()
	name := c.Param("name")
	subName := c.Param("subname")
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	err := t.pubsub.DeleteSnapshots(ctx, name, subName, topic)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.Status(http.StatusOK)
	metrics.GetMetrics().SnapshotsHistogramVec.WithLabelValues("delete").Observe(time.Since(start).Seconds())
}

// Seek to a snapshot
func (t *Server) Seek(c *gin.Context) {
	start := time.Now()
	name := c.Param("name")
	subName := c.Param("subname")
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	sub, err := t.pubsub.Seek(ctx, name, subName, topic)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, sub)
	metrics.GetMetrics().SnapshotsHistogramVec.WithLabelValues("seek").Observe(time.Since(start).Seconds())
}
