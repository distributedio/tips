package main

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	v := m.Run()
	os.Exit(v)
}
