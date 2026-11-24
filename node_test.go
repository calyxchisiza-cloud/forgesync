package forgesync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSyncCopiesOfflineWrites(t *testing.T) {
	a := mustNode(t, "a")
	b := mustNode(t, "b")
	if _, err := a.Put("order:1", []byte("packed")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put("order:2", []byte("shipped")); err != nil {
		t.Fatal(err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	if err := sameNode(a, b); err != nil {
		t.Fatal(err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	hist, err := a.History("order:1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("history %d", len(hist))
	}
}

func TestLaterWriteWinsAndEarlierStays(t *testing.T) {
	a := mustNode(t, "a")
	b := mustNode(t, "b")
	if _, err := a.Put("greeting", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put("greeting", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	got, ok, err := a.Get("greeting")
	if err != nil || !ok || string(got) != "hi" {
		t.Fatalf("got %q ok %v err %v", got, ok, err)
	}
	hist, err := a.History("greeting")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 || string(hist[0].Value) != "hello" || string(hist[1].Value) != "hi" {
		t.Fatalf("%+v", hist)
	}
	if _, err := a.Put("greeting", []byte("hello again")); err != nil {
		t.Fatal(err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	for _, n := range []*Node{a, b} {
		got, ok, err := n.Get("greeting")
		if err != nil || !ok || string(got) != "hello again" {
			t.Fatalf("%s %q ok %v err %v", n.ID(), got, ok, err)
		}
	}
}

func TestDeleteRemovesTheKey(t *testing.T) {
	a := mustNode(t, "a")
	b := mustNode(t, "b")
	if _, err := a.Put("box", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Delete("box"); err != nil {
		t.Fatal(err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := a.Get("box"); err != nil || ok {
		t.Fatalf("ok %v err %v", ok, err)
	}
	if _, ok, err := b.Get("box"); err != nil || ok {
		t.Fatalf("ok %v err %v", ok, err)
	}
	if _, err := a.Put("empty", []byte{}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := a.Get("empty")
	if err != nil || !ok || len(got) != 0 {
		t.Fatalf("empty %q ok %v err %v", got, ok, err)
	}
}

func TestGapDoesNotSkipAChange(t *testing.T) {
	a := mustNode(t, "a")
	b := mustNode(t, "b")
	first, err := a.Put("greeting", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	second := Change{Origin: "a", Seq: 2, Key: "greeting", Value: []byte("two"), Time: 2}
	if err := b.Apply([]Change{second}); !errors.Is(err, ErrGap) {
		t.Fatalf("gap: %v", err)
	}
	if _, ok, err := b.Get("greeting"); err != nil || ok {
		t.Fatalf("stored a gapped change ok %v err %v", ok, err)
	}
	if err := b.Apply([]Change{first, second}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := b.Get("greeting")
	if err != nil || !ok || string(got) != "two" {
		t.Fatalf("%q ok %v err %v", got, ok, err)
	}
	if err := b.Apply([]Change{first, second}); err != nil {
		t.Fatal(err)
	}
	hist, err := b.History("greeting")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("history %d", len(hist))
	}
}

func TestThreeNodesConverge(t *testing.T) {
	a := mustNode(t, "a")
	b := mustNode(t, "b")
	c := mustNode(t, "c")
	if _, err := a.Put("k", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	if err := Sync(b, c); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get("k")
	if err != nil || !ok || string(got) != "1" {
		t.Fatalf("%q ok %v err %v", got, ok, err)
	}
	if _, err := c.Put("k", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := Sync(c, a); err != nil {
		t.Fatal(err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	for _, n := range []*Node{a, b, c} {
		got, ok, err := n.Get("k")
		if err != nil || !ok || string(got) != "2" {
			t.Fatalf("%s %q ok %v err %v", n.ID(), got, ok, err)
		}
	}
}

func TestRestartKeepsTheLog(t *testing.T) {
	dir := t.TempDir()
	n, err := Open(dir, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Put("greeting", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, "b"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("other id: %v", err)
	}
	n, err = Open(dir, "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	got, ok, err := n.Get("greeting")
	if err != nil || !ok || string(got) != "hello" {
		t.Fatalf("%q ok %v err %v", got, ok, err)
	}
	b := mustNode(t, "b")
	if err := Sync(n, b); err != nil {
		t.Fatal(err)
	}
	got, ok, err = b.Get("greeting")
	if err != nil || !ok || string(got) != "hello" {
		t.Fatalf("replica %q ok %v err %v", got, ok, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Put("greeting", []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestTornTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	body, err := json.Marshal(Change{Origin: "a", Seq: 1, Key: "greeting", Value: []byte("hello"), Time: 1})
	if err != nil {
		t.Fatal(err)
	}
	raw := append(append(body, '\n'), []byte(`{"origin":"a","seq":2`)...)
	if err := os.WriteFile(filepath.Join(dir, "id"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, logName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	n, err := Open(dir, "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	got, ok, err := n.Get("greeting")
	if err != nil || !ok || string(got) != "hello" {
		t.Fatalf("%q ok %v err %v", got, ok, err)
	}
	disk, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	if string(disk) != string(body)+"\n" {
		t.Fatalf("log %s", disk)
	}
}

func TestRejectedChanges(t *testing.T) {
	a := mustNode(t, "a")
	b := mustNode(t, "b")
	if _, err := a.Put("bad key", []byte("x")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("key: %v", err)
	}
	if err := Sync(a, a); !errors.Is(err, ErrSameNode) {
		t.Fatalf("same: %v", err)
	}
	if _, err := New(""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("id: %v", err)
	}
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
}

func TestDialSyncsABatch(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir, "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	for i := 0; i < 300; i++ {
		if _, err := a.Put(fmt.Sprintf("k%03d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	b := mustNode(t, "b")
	if _, err := b.Put("from-b", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	srv, err := a.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	if err := Dial(srv.Addr(), b); err != nil {
		t.Fatal(err)
	}
	if err := sameNode(a, b); err != nil {
		t.Fatal(err)
	}
	if err := Dial(srv.Addr(), b); err != nil {
		t.Fatal(err)
	}
	hist, err := a.History("k000")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("history grew to %d", len(hist))
	}
}

func TestConcurrentPutsConverge(t *testing.T) {
	a := mustNode(t, "a")
	b := mustNode(t, "b")
	ctx, cancel := context.WithCancel(context.Background())
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		for ctx.Err() == nil {
			_ = Sync(a, b)
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, _ = a.Put(fmt.Sprintf("a%02d", i), []byte("a"))
			_, _ = a.Put("shared", []byte("from-a"))
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _ = b.Put(fmt.Sprintf("b%02d", i), []byte("b"))
			_, _ = b.Put("shared", []byte("from-b"))
		}(i)
	}
	wg.Wait()
	cancel()
	bg.Wait()
	if err := Sync(a, b); err != nil {
		t.Fatal(err)
	}
	if err := sameNode(a, b); err != nil {
		t.Fatal(err)
	}
	items, err := a.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 81 {
		t.Fatalf("items %d", len(items))
	}
}

func mustNode(t *testing.T, id string) *Node {
	t.Helper()
	n, err := New(id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

func sameNode(a, b *Node) error {
	left, err := a.List()
	if err != nil {
		return err
	}
	right, err := b.List()
	if err != nil {
		return err
	}
	if len(left) != len(right) {
		return fmt.Errorf("len %d and %d", len(left), len(right))
	}
	for i := range left {
		if left[i].Key != right[i].Key || !bytes.Equal(left[i].Value, right[i].Value) {
			return fmt.Errorf("item %d %q %q vs %q %q", i, left[i].Key, left[i].Value, right[i].Key, right[i].Value)
		}
	}
	return nil
}
