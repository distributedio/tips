package main

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shafreeck/tips"
	"github.com/shafreeck/tips/conf"
	"github.com/twinj/uuid"
)

type Server struct {
	router *gin.Engine
	pubsub tips.Pubsub
	ctx    context.Context
	cancel context.CancelFunc

	certFile   string
	keyFile    string
	httpServer *http.Server
}

func NewServer(conf *conf.Server, pubsub tips.Pubsub) *Server {
	router := gin.New()

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		ctx:        ctx,
		cancel:     cancel,
		router:     router,
		pubsub:     pubsub,
		httpServer: &http.Server{Handler: router},
	}
	s.initRouter()
	return s
}

func (s *Server) initRouter() {
	s.router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"Code": 404, "Reason": "thord: Page not found. Resource you request may not exist."})
	})
	s.router.PUT("/v1/topics/:topic", s.CreateTopic)
	s.router.GET("/v1/topics/:topic", s.Topic)
	s.router.DELETE("/v1/topics/:topic", s.Destroy)

	s.router.POST("/v1/messages/topic", s.Publish)
	s.router.POST("/v1/messages/ack", s.Ack)

	s.router.PUT("/v1/subscriptions/:subname/:topic", s.Subscribe)
	s.router.DELETE("/v1/subscriptions/:subname/:topic", s.Unsubscribe)
	s.router.GET("/v1/subscriptions/:subname", s.Subscription)
	s.router.POST("/v1/subscriptions/:subname", s.Pull)

	s.router.PUT("/v1/snapshots/:name/:subname", s.CreateSnapshots)
	s.router.DELETE("/v1/snapshots/:name/:subname", s.DeleteSnapshots)
	s.router.POST("/v1/snapshots/:name", s.Seek)
}

func (s *Server) Serve(lis net.Listener) error {
	return s.httpServer.ServeTLS(lis, s.certFile, s.keyFile)
}

func (s *Server) Stop() error {
	s.cancel()
	return s.httpServer.Close()
}

func (s *Server) GracefulStop() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
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
