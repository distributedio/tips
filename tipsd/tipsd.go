package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shafreeck/tips"
)

//CreateTopic 创建一个topic 未知指定topic name 系统自动生成一个 返回给客户端topic名字
func (s *Server) CreateTopic(c *gin.Context) {
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	if _, err := s.pubsub.CreateTopic(ctx, topic); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, topic)
	return
}

//Topic 查询topic 订阅信息
func (t *Server) Topic(c *gin.Context) {
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
}

//Destroy 销毁topic
//禁止 topic 为空
func (t *Server) Destroy(c *gin.Context) {
	topic := c.Param("topic")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	if err := t.pubsub.Destroy(ctx, topic); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.Status(http.StatusOK)
}

//Publish 消息下发 支持批量下发,返回下发成功的msgids
//msgids 返回的序列和下发消息序列保持一直
//禁止 topic 和 msgs 未空
func (t *Server) Publish(c *gin.Context) {
	pub := &struct {
		Topic    string
		Messages []string
	}{}
	if err := c.BindJSON(pub); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if len(pub.Topic) == 0 {
		fail(c, http.StatusBadRequest, errors.New("topic is not null"))
		return
	}
	if len(pub.Messages) == 0 {
		fail(c, http.StatusBadRequest, errors.New("msg is not null"))
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	msgids, err := t.pubsub.Publish(ctx, pub.Messages, pub.Topic)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, msgids)
}

//Ack 回复消息ack 禁止msgids为空
func (t *Server) Ack(c *gin.Context) {
	req := struct {
		Msgid   string
		SubName string
		Topic   string
	}{}
	if err := c.BindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	err := t.pubsub.Ack(ctx, req.Msgid, req.Topic, req.SubName)
	if err != nil {
		if ErrNotFound(err) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.Status(http.StatusOK)
}

//Subscribe 指定topic 和 subscription 订阅关系
//禁止topic 和subscition 为空
func (t *Server) Subscribe(c *gin.Context) {
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
}

//Unsubscribe 指定topic 和 subscription 订阅关系
//禁止topic 和subscition 为空
func (t *Server) Unsubscribe(c *gin.Context) {
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
}

//Subscription 查询当前subscription的信息
//禁止subname 为空
//返回 TODO
/*
func (t *Server) Subscription(c *gin.Context) {
	subName := c.Param("subname")
	if len(subName) == 0 {
		c.JSON(http.StatusBadRequest, "subname is not null")
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
		_, err := t.pubsub.Subscription(ctx, subName)
		if err != nil {
			// if err == keyNotFound {
			// c.JSON(http.StatusOK, fmt.Sprintf(NameNotFount, subName))
			// return
			// }
			c.JSON(http.StatusInternalServerError, err.Error())
			return
		}
	//TODO json
}
*/

//Pull 拉取消息
//禁止topic subName 为空,limit 必须大于0
//如果没有指定消息拉去超时间，默认1s 超时,超时单位默认为s
//返回下一次拉去的位置
func (t *Server) Pull(c *gin.Context) {
	req := &struct {
		Limit   int64
		Timeout int64
		OffAck  bool
		Offset  string
	}{}
	if err := c.BindJSON(req); err != nil {
		c.JSON(http.StatusBadRequest, err.Error())
		return
	}
	if req.Limit <= 0 {
		req.Limit = 1
	}
	if req.Timeout == 0 {
		req.Timeout = 1
	}

	t1 := time.Duration(req.Timeout) * time.Second
	pReq := &tips.PullReq{
		SubName: c.Param("subname"),
		Topic:   c.Param("topic"),
		Limit:   req.Limit,
		OffAck:  req.OffAck,
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
}

//CreateSnapshots 创建一个时间的点
//禁止subname 为空
//name 未指定默认，系统自动生成
//返回创建snapshots名字
func (t *Server) CreateSnapshots(c *gin.Context) {
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
}

//DeleteSnapshots 删除snapshots
//禁止name 和subname 为空
func (t *Server) DeleteSnapshots(c *gin.Context) {
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
}

//GetSnapshots 获取snapshots 配置
//禁止subname 为空
func (t *Server) GetSnapshots(c *gin.Context) {
	subName := c.Query("subname")
	if len(subName) == 0 {
		c.JSON(http.StatusBadRequest, "subName is not null")
		return
	}
	/*
		ctx, cancel := context.WithCancel(t.ctx)
		defer cancel()

			_, err := t.pubsub.GetSnapshots(ctx, subName)
			if err != nil {
				c.JSON(http.StatusInternalServerError, err.Error())
				return
			}
	*/
	//TODO struct
}

//Seek 获取订阅通道 snapshots开始位置
func (t *Server) Seek(c *gin.Context) {
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
}
