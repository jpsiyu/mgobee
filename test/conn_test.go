package test

import (
	"testing"

	"github.com/jpsiyu/mgobee"
)

func TestSmartConnect(t *testing.T) {
	bee := mgobee.Create("mydb", "me", "123", []string{"mongodb://mongo:27017", "mongodb://localhost:27017"})
	dbchan := make(chan error)
	go bee.SmartConnect(dbchan)
	err := <-dbchan
	if err != nil {
		t.Error(err)
	}
}