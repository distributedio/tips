package main

import (
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/shafreeck/tips"
	"github.com/shafreeck/tips/conf"
)

var addr = "127.0.0.1:12345"

func TestMain(m *testing.M) {
	var conf *conf.Server
	var pubsub tips.Pubsub
	pubsub := tips.MockPubsub()
	server := NewServer(conf, pubsub)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	go server.Serve(lis)
	time.Sleep(time.Second)
	v := m.Run()
	os.Exit(v)
}
