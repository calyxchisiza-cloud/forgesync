package main

import (
	"fmt"
	"os"

	"forgesync"
)

func main() {
	a, err := forgesync.New("a")
	if err != nil {
		fail(err)
	}
	b, err := forgesync.New("b")
	if err != nil {
		fail(err)
	}
	if _, err := a.Put("greeting", []byte("hello")); err != nil {
		fail(err)
	}
	if _, err := b.Put("greeting", []byte("hi")); err != nil {
		fail(err)
	}
	if _, err := a.Put("note", []byte("left")); err != nil {
		fail(err)
	}
	if err := forgesync.Sync(a, b); err != nil {
		fail(err)
	}
	if err := show(a, b); err != nil {
		fail(err)
	}
	if _, err := a.Put("greeting", []byte("hello again")); err != nil {
		fail(err)
	}
	if err := forgesync.Sync(a, b); err != nil {
		fail(err)
	}
	if err := show(a, b); err != nil {
		fail(err)
	}
}

func show(nodes ...*forgesync.Node) error {
	for _, n := range nodes {
		items, err := n.List()
		if err != nil {
			return err
		}
		for _, item := range items {
			fmt.Printf("%s %s %s\n", n.ID(), item.Key, item.Value)
		}
	}
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
