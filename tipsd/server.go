package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shafreeck/tips"
	"github.com/shafreeck/tips/conf"
	"github.com/twinj/uuid"
)

type Server struct {
	router *gin.Engine
	pubsub *tips.Tips
	ctx    context.Context
	cancel context.CancelFunc

	certFile   string
	keyFile    string
	httpServer *http.Server
}

func NewServer(conf *conf.Server, pubsub *tips.Tips) *Server {
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
	// s.router.Use(AccessLoggerFunc(zap.L()), gin.Recovery())
	s.router.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{"Reason": "tips: Page not found. Resource you request may not exist."})
	})
	s.router.PUT("/v1/topics/:topic", s.CreateTopic)
	s.router.GET("/v1/topics/:topic", s.Topic)
	s.router.DELETE("/v1/topics/:topic", s.Destroy)

	s.router.POST("/v1/messages/topic", s.Publish)
	s.router.POST("/v1/messages/ack", s.Ack)

	s.router.PUT("/v1/subscriptions/:subname/:topic", s.Subscribe)
	s.router.DELETE("/v1/subscriptions/:subname/:topic", s.Unsubscribe)
	// s.router.GET("/v1/subscriptions/:subname", s.Subscription)
	s.router.POST("/v1/subscriptions/:subname/:topic", s.Pull)

	s.router.PUT("/v1/snapshots/:name/:subname/:topic", s.CreateSnapshots)
	s.router.DELETE("/v1/snapshots/:name/:subname/:topic", s.DeleteSnapshots)
	s.router.POST("/v1/snapshots/:name/:subname/:topic", s.Seek)
}

func (s *Server) Serve(lis net.Listener) error {
	// return s.httpServer.ServeTLS(lis, s.certFile, s.keyFile)
	return s.httpServer.Serve(lis)
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

func GenName() string {
	return uuid.NewV4().String()
}

func (t *Server) pull(ctx context.Context, req *tips.PullReq, timeout time.Duration) ([]*tips.Message, error) {
	tick := time.Tick(timeout)
	var msgs []*tips.Message
	var err error
	for {
		select {
		case <-tick:
			return msgs, nil
		default:
			msgs, err = t.pubsub.Pull(ctx, req)
			if err != nil {
				return nil, err
			}
		}
		if len(msgs) != 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	return msgs, nil
}

func ErrNotFound(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

type Error struct {
	Reason string `json:"reason"`
}

func fail(c *gin.Context, httpStatus int, err error) {
	e := Error{Reason: err.Error()}
	c.JSON(httpStatus, e)
}
