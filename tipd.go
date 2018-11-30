package tipd

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type tipd struct {
	router *gin.Engine
	pubsub Pubsub
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTipd(pubsub Pubsub) *tipd {
	router := gin.New()
	router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"Code": 404, "Reason": "thord: Page not found. Resource you request may not exist."})
	})
	ctx, cancel := context.WithCancel(context.Background())
	t := &tipd{
		ctx:    ctx,
		cancel: cancel,
		router: router,
		pubsub: pubsub,
	}
	router.GET("/v1/topic", t.CreateTopic)
	router.GET("/v1/topic/subscription", t.Topic)
	router.GET("/v1/topic", t.Destroy)

	router.GET("/v1/topic", t.Publish)
	router.GET("/v1/ack", t.Ack)

	router.GET("/v1/subscription", t.Subscribe)
	router.GET("/v1/subscription", t.Unsubscribe)
	router.GET("/v1/subscription", t.Subscription)
	router.GET("/v1/subscription", t.Pull)

	router.GET("/v1/snapshots", t.CreateSnapshots)
	router.GET("/v1/snapshots", t.GetSnapshots)
	router.GET("/v1/snapshots", t.DeleteSnapshots)
	return t
}

//CreateTopic 创建一个topic 未知指定topic name 系统自动生成一个 返回给客户端topic名字
func (t *tipd) CreateTopic(c *gin.Context) {
	topic := c.Query("topic")
	if len(topic) == 0 {
		topic = GenName()
	}
	if err := t.pubsub.CreateTopic(t.ctx, topic); err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, topic)
	return
}

//Topic 查询topic 订阅信息
//禁止 topic 为空
func (t *tipd) Topic(c *gin.Context) {
	topic := c.Query("topic")
	if len(topic) == 0 {
		c.JSON(http.StatusBadRequest, "topic is not null")
		return
	}

	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	subName, err := t.pubsub.Topic(ctx, topic)
	if err != nil {
		// if err == keyNotFound {
		// c.JSON(http.StatusOK, fmt.Sprintf(NameNotFount, subName))
		// return
		// }
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	//TODO subName
	c.JSON(http.StatusBadRequest, subName)
}

//Destroy 销毁topic
//禁止 topic 为空
func (t *tipd) Destroy(c *gin.Context) {
	topic := c.Query("topic")
	if len(topic) == 0 {
		c.JSON(http.StatusBadRequest, "topic is not null")
		return
	}

	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	if err := t.pubsub.Destroy(ctx, topic); err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.Status(http.StatusOK)
}

//Publish 消息下发 支持批量下发,返回下发成功的msgids
//msgids 返回的序列和下发消息序列保持一直
//禁止 topic 和 msgs 未空
func (t *tipd) Publish(c *gin.Context) {
	msgs := c.QueryArray("messages")
	topic := c.Query("topic")
	if len(topic) == 0 {
		c.JSON(http.StatusBadRequest, "topic is not null")
		return
	}
	if len(msgs) == 0 {
		c.JSON(http.StatusBadRequest, "msgs is not null")
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	msgids, err := t.pubsub.Publish(ctx, msgs, topic)
	if err != nil {
		// if err == keyNotFound {
		// c.JSON(http.StatusOK, fmt.Sprintf(NameNotFount, subName))
		// return
		// }
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, msgids)
}

//Ack 回复消息ack 禁止msgids为空
func (t *tipd) Ack(c *gin.Context) {
	msgids := c.QueryArray("msgids")
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	err := t.pubsub.Ack(ctx, msgids)
	if err != nil {
		// if err == keyNotFound {
		// c.JSON(http.StatusOK, fmt.Sprintf(NameNotFount, subName))
		// return
		// }
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.Status(http.StatusOK)
}

//Subscribe 指定topic 和 subscription 订阅关系
//禁止topic 和subscition 为空
func (t *tipd) Subscribe(c *gin.Context) {
	subName := c.Query("subName")
	if len(subName) == 0 {
		c.JSON(http.StatusBadRequest, "subname is not null")
		return
	}
	topic := c.Query("topic")
	if len(topic) == 0 {
		c.JSON(http.StatusBadRequest, "topic is not null")
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	index, err := t.pubsub.Subscribe(ctx, subName, topic)
	if err != nil {
		// if err == keyNotFound {
		// c.JSON(http.StatusOK, fmt.Sprintf(NameNotFount, subName))
		// return
		// }
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, index)
}

//Unsubscribe 指定topic 和 subscription 订阅关系
//禁止topic 和subscition 为空
func (t *tipd) Unsubscribe(c *gin.Context) {
	subName := c.Query("subName")
	if len(subName) == 0 {
		c.JSON(http.StatusBadRequest, "subName is not null")
		return
	}
	topic := c.Query("topic")
	if len(topic) == 0 {
		c.JSON(http.StatusBadRequest, "topic is not null")
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	err := t.pubsub.Unsubscribe(ctx, subName, topic)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.Status(http.StatusOK)
}

//Subscription 查询当前subscription的信息
//禁止subname 为空
//返回 TODO
func (t *tipd) Subscription(c *gin.Context) {
	subName := c.Query("subName")
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

//Pull 拉取消息
//禁止topic subName 为空,limit 必须大于0
//如果没有指定消息拉去超时间，默认1s 超时,超时单位默认为s
//返回下一次拉去的位置
func (t *tipd) Pull(c *gin.Context) {
	subName := c.Query("subName")
	if len(subName) == 0 {
		c.JSON(http.StatusBadRequest, "subname is not null")
		return
	}
	limit := c.GetInt64("limit")
	if limit <= 0 {
		c.JSON(http.StatusBadRequest, "limit can less than zero")
		return
	}
	cursor := c.GetInt64("cursor")
	ack := c.GetBool("ack")
	timeout := c.GetInt("timeout")
	t1 := time.Duration(timeout) * time.Second
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	//TODO
	_, _, err := t.pull(ctx, subName, cursor, limit, ack, t1)
	if err != nil {
		// if err == keyNotFound {
		// c.JSON(http.StatusOK, fmt.Sprintf(NameNotFount, subName))
		// return
		// }
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
}

//CreateSnapshots 创建一个时间的点
//禁止subname 为空
//name 未指定默认，系统自动生成
//返回创建snapshots名字
func (t *tipd) CreateSnapshots(c *gin.Context) {

	subName := c.Query("subName")
	if len(subName) == 0 {
		c.JSON(http.StatusBadRequest, "subname is not null")
		return
	}
	name := c.Query("name")
	if len(name) == 0 {
		name = GenName()
		return
	}

	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	_, err := t.pubsub.CreateSnapshots(ctx, name, subName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, name)
}

//DeleteSnapshots 删除snapshots
//禁止name 和subname 为空
func (t *tipd) DeleteSnapshots(c *gin.Context) {
	name := c.Query("name")
	if len(name) == 0 {
		c.JSON(http.StatusBadRequest, "name is not null")
		return
	}
	subName := c.Query("subName")
	if len(subName) == 0 {
		c.JSON(http.StatusBadRequest, "subName is not null")
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	err := t.pubsub.DeleteSnapshots(ctx, name, subName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.Status(http.StatusOK)
}

//GetSnapshots 获取snapshots 配置
//禁止subname 为空
func (t *tipd) GetSnapshots(c *gin.Context) {
	subName := c.Query("subName")
	if len(subName) == 0 {
		c.JSON(http.StatusBadRequest, "subName is not null")
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	_, err := t.pubsub.GetSnapshots(ctx, subName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	//TODO struct
}

//Seek 获取订阅通道 snapshots开始位置
//禁止那么 为空
func (t *tipd) Seek(c *gin.Context) {
	name := c.Query("name")
	if len(name) == 0 {
		c.JSON(http.StatusBadRequest, "name is not null")
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	_, err := t.pubsub.Seek(ctx, name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	//TODO struct
}
