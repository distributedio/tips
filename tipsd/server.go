package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tipsio/tips"
	"github.com/tipsio/tips/conf"
)

// Server HTTP service structure
// currently, it is an HTTP service and HTTPS server is not supported temporarily
type Server struct {
	router *gin.Engine
	pubsub *tips.Tips
	ctx    context.Context
	cancel context.CancelFunc

	certFile   string
	keyFile    string
	httpServer *http.Server
}

// NewServer creates a server
func NewServer(conf *conf.Server, pubsub *tips.Tips) *Server {
	router := gin.New()

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		ctx:        ctx,
		cancel:     cancel,
		router:     router,
		pubsub:     pubsub,
		certFile:   conf.Cert,
		keyFile:    conf.Key,
		httpServer: &http.Server{Handler: router},
	}

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

	s.router.POST("/v1/messages/topics/:topic", s.Publish)
	s.router.POST("/v1/messages/ack/:topic/:subname/:msgid", s.Ack)

	s.router.PUT("/v1/subscriptions/:topic/:subname", s.Subscribe)
	s.router.DELETE("/v1/subscriptions/:topic/:subname", s.Unsubscribe)
	// s.router.GET("/v1/subscriptions/:subname", s.Subscription)
	s.router.POST("/v1/subscriptions/:topic/:subname", s.Pull)

	s.router.PUT("/v1/snapshots/:topic/:subname/:name", s.CreateSnapshots)
	s.router.DELETE("/v1/snapshots/:topic/:subname/:name", s.DeleteSnapshots)
	s.router.POST("/v1/snapshots/:topic/:subname/:name", s.Seek)
}

// Serve accepts incoming connections on the Listener l, creating a
// new service goroutine for each.
func (s *Server) Serve(lis net.Listener) error {
	s.initRouter()
  
	if s.certFile != "" && s.keyFile != "" {
		return s.httpServer.ServeTLS(lis, s.certFile, s.keyFile)
	}
	return s.httpServer.Serve(lis)
}

// Stop immediately closes all active net.Listeners and any
// connections in state StateNew, StateActive, or StateIdle
func (s *Server) Stop() error {
	s.cancel()
	return s.httpServer.Close()
}

// GracefulStop gracefully shuts down the server without interrupting any
// active connections
func (s *Server) GracefulStop() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func (t *Server) pull(ctx context.Context, req *tips.PullReq, timeout time.Duration) ([]*tips.Message, error) {
	tick := time.Tick(timeout)
	var msgs []*tips.Message
	var err error
	// when no data is available, try again every 100ms until the tick is out.
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

// ErrNotFound returns true if the error is caused by resource missing
func ErrNotFound(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

// Error wraps a http server error
type Error struct {
	Reason string `json:"reason"`
}

func fail(c *gin.Context, httpStatus int, err error) {
	e := Error{Reason: err.Error()}
	c.JSON(httpStatus, e)
}
