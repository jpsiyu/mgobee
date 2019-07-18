package test

import (
	"testing"

	"github.com/jpsiyu/mgobee"
)

func TestCreate(t *testing.T) {
	_ = mgobee.Create("mydb", "me", "123", []string{})
}

func TestConnect(t *testing.T) {
	bee := mgobee.Create("mydb", "me", "123", []string{})
	err := bee.Connect("mongodb://localhost:27017")
	if err != nil {
		t.Error(err)
	}
}

func TestPing(t *testing.T) {
	bee := mgobee.Create("mydb", "me", "123", []string{})
	err := bee.Connect("mongodb://localhost:27017")
	if err != nil {
		t.Error(err)
	}
	err = bee.Ping()
	if err != nil {
		t.Error(err)
	}
}

func TestSmartConnect(t *testing.T) {
	bee := mgobee.Create("mydb", "me", "123", []string{"mongodb://mongo:27017", "mongodb://localhost:27017"})
	err := bee.SmartConnect()
	if err != nil {
		t.Error(err)
	}
}

func TestInsert(t *testing.T) {
	bee := mgobee.Create("mydb", "me", "123", []string{})
	err := bee.Connect("mongodb://localhost:27017")
	if err != nil {
		t.Error(err)
	}
	type Person struct {
		Name string
	}
	doc := Person{
		Name: "hi",
	}
	err = bee.Insert(&doc, "hello")
	if err != nil {
		t.Error(err)
	}
}
