package forgesync

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

const (
	maxValue = 1 << 20
	maxBatch = 128
	maxFrame = 8 << 20
)

var (
	ErrClosed   = errors.New("node is closed")
	ErrInvalid  = errors.New("change is not valid")
	ErrGap      = errors.New("change sequence has a gap")
	ErrSameNode = errors.New("node ids match")
	ErrTooLarge = errors.New("sync frame is too large")
)

// Change is one write or delete. Origin and Seq identify it forever.
// A higher Time wins. Equal times break toward the greater Origin, then Seq.
type Change struct {
	Origin  string `json:"origin"`
	Seq     uint64 `json:"seq"`
	Key     string `json:"key"`
	Value   []byte `json:"value,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
	Time    int64  `json:"time"`
}

// Cursor is the highest contiguous sequence a node has from each origin.
type Cursor map[string]uint64

// Item is the current value of one key.
type Item struct {
	Key   string
	Value []byte
}

// Node is one replica. New keeps the log in memory. Open stores it in a directory.
type Node struct {
	id     string
	file   writeSync
	cursor Cursor
	clock  int64
	log    []Change
	win    map[string]Change
	mu     sync.Mutex
	closed bool
}

type writeSync interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

// New starts an in-memory node. The id is permanent for that replica.
func New(id string) (*Node, error) {
	if err := validID(id); err != nil {
		return nil, err
	}
	return newNode(id), nil
}

func newNode(id string) *Node {
	return &Node{
		id:     id,
		cursor: Cursor{},
		win:    make(map[string]Change),
	}
}

func (n *Node) ID() string { return n.id }

// Put stores a value under key. An empty value is kept. It is not a delete.
func (n *Node) Put(key string, value []byte) (Change, error) {
	return n.write(key, value, false)
}

// Delete removes key. The delete is itself a change and can win over an older put.
func (n *Node) Delete(key string) (Change, error) {
	return n.write(key, nil, true)
}

func (n *Node) write(key string, value []byte, deleted bool) (Change, error) {
	if err := validKey(key); err != nil {
		return Change{}, err
	}
	if len(value) > maxValue {
		return Change{}, fmt.Errorf("%w: value is too large", ErrInvalid)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return Change{}, ErrClosed
	}
	n.clock++
	c := Change{
		Origin:  n.id,
		Seq:     n.cursor[n.id] + 1,
		Key:     key,
		Deleted: deleted,
		Time:    n.clock,
	}
	if !deleted {
		c.Value = append([]byte(nil), value...)
	}
	if err := n.applyLocked(c); err != nil {
		n.clock--
		return Change{}, err
	}
	return cloneChange(c), nil
}

// Get returns the winning value. The second result is false when the key is
// absent or the winning change deleted it.
func (n *Node) Get(key string) ([]byte, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, false, ErrClosed
	}
	w, ok := n.win[key]
	if !ok || w.Deleted {
		return nil, false, nil
	}
	return append([]byte(nil), w.Value...), true, nil
}

// List returns the current keys in order. Deleted keys are left out.
func (n *Node) List() ([]Item, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, ErrClosed
	}
	out := make([]Item, 0, len(n.win))
	for key, w := range n.win {
		if w.Deleted {
			continue
		}
		out = append(out, Item{Key: key, Value: append([]byte(nil), w.Value...)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// History returns every change for key, oldest first.
func (n *Node) History(key string) ([]Change, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, ErrClosed
	}
	out := make([]Change, 0)
	for _, c := range n.log {
		if c.Key == key {
			out = append(out, cloneChange(c))
		}
	}
	sort.Slice(out, func(i, j int) bool { return newer(out[j], out[i]) })
	return out, nil
}

// Cursor is a copy of the sequences this node has applied.
func (n *Node) Cursor() (Cursor, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, ErrClosed
	}
	return copyCursor(n.cursor), nil
}

// Missing returns changes this node has that cursor does not, origin then sequence.
func (n *Node) Missing(cursor Cursor) ([]Change, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, ErrClosed
	}
	return n.missingLocked(cursor), nil
}

func (n *Node) missingLocked(cursor Cursor) []Change {
	out := make([]Change, 0)
	for _, c := range n.log {
		if c.Seq > cursor[c.Origin] {
			out = append(out, cloneChange(c))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Origin != out[j].Origin {
			return out[i].Origin < out[j].Origin
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// Apply stores changes in order. A sequence that skips ahead returns ErrGap
// after the prefix that did fit. A change this node already has is ignored.
func (n *Node) Apply(changes []Change) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	for _, c := range changes {
		if err := n.applyLocked(c); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) applyLocked(c Change) error {
	if err := validChange(c); err != nil {
		return err
	}
	have := n.cursor[c.Origin]
	if c.Seq <= have {
		return nil
	}
	if c.Seq != have+1 {
		return ErrGap
	}
	if err := n.append(c); err != nil {
		return err
	}
	n.remember(c)
	return nil
}

func (n *Node) remember(c Change) {
	c = cloneChange(c)
	if !c.Deleted && c.Value == nil {
		c.Value = []byte{}
	}
	n.log = append(n.log, c)
	n.cursor[c.Origin] = c.Seq
	if c.Time > n.clock {
		n.clock = c.Time
	}
	cur, ok := n.win[c.Key]
	if !ok || newer(c, cur) {
		n.win[c.Key] = c
	}
}

func newer(a, b Change) bool {
	if a.Time != b.Time {
		return a.Time > b.Time
	}
	if a.Origin != b.Origin {
		return a.Origin > b.Origin
	}
	return a.Seq > b.Seq
}

// Sync copies every missing change in both directions.
// The two nodes must have different ids. One pass is enough while both are quiet.
func Sync(a, b *Node) error {
	if a == nil || b == nil {
		return ErrInvalid
	}
	if a.id == b.id {
		return ErrSameNode
	}
	left, right := a, b
	if a.id > b.id {
		left, right = b, a
	}
	left.mu.Lock()
	right.mu.Lock()
	defer right.mu.Unlock()
	defer left.mu.Unlock()
	if left.closed || right.closed {
		return ErrClosed
	}
	ab := left.missingLocked(right.cursor)
	ba := right.missingLocked(left.cursor)
	if err := right.applyAll(ab); err != nil {
		return err
	}
	if err := left.applyAll(ba); err != nil {
		return err
	}
	return nil
}

func (n *Node) applyAll(changes []Change) error {
	for _, c := range changes {
		if err := n.applyLocked(c); err != nil {
			return err
		}
	}
	return nil
}

// Close releases the log. Further reads and writes fail.
func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
	if n.file == nil {
		return nil
	}
	err := n.file.Close()
	n.file = nil
	return err
}

func cloneChange(c Change) Change {
	c.Value = append([]byte(nil), c.Value...)
	return c
}

func copyCursor(c Cursor) Cursor {
	out := make(Cursor, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}

func bump(cursor Cursor, changes []Change) {
	for _, c := range changes {
		if c.Seq > cursor[c.Origin] {
			cursor[c.Origin] = c.Seq
		}
	}
}

func validID(id string) error {
	if id == "" || len(id) > 128 {
		return fmt.Errorf("%w: id is not a node name", ErrInvalid)
	}
	for i, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && (r >= '0' && r <= '9' || r == '.' || r == '-'):
		default:
			return fmt.Errorf("%w: id is not a node name", ErrInvalid)
		}
	}
	return nil
}

func validKey(key string) error {
	if key == "" || len(key) > 256 {
		return fmt.Errorf("%w: key is not valid", ErrInvalid)
	}
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '.' || r == '-' || r == ':':
		default:
			return fmt.Errorf("%w: key is not valid", ErrInvalid)
		}
	}
	return nil
}

func validChange(c Change) error {
	if err := validID(c.Origin); err != nil {
		return err
	}
	if err := validKey(c.Key); err != nil {
		return err
	}
	if c.Seq == 0 || c.Time < 0 || len(c.Value) > maxValue {
		return ErrInvalid
	}
	if c.Deleted && len(c.Value) > 0 {
		return ErrInvalid
	}
	return nil
}
