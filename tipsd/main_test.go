package main

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	server := NewServer()
	server.Serve()
	v := m.Run()
	os.Exit(v)
}
