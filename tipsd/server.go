package main

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/twinj/uuid"
)

type Server struct {
	router   *gin.Engine
	pubsub   Pubsub
	ctx      context.Context
	cancel   context.CancelFunc
	certFile string
	keyFile  string
	*http.Server
}

func NewServer(pubsub Pubsub) *Server {
	router := gin.New()
	router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"Code": 404, "Reason": "thord: Page not found. Resource you request may not exist."})
	})
	ctx, cancel := context.WithCancel(context.Background())
	t := &Server{
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

func (t *Server) Serve(lis net.Listener) error {
	return t.ServeTLS(lis, t.certFile, t.keyFile)
}

func (t *Server) Stop() error {
	return t.Server.Close()
}

func (s *Server) GracefulStop() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return s.Server.Shutdown(ctx)
}

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
func (t *Server) pull(ctx context.Context, subname string, index, limit int64, ack bool, timeout time.Duration) ([]string, int64, error) {
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
