package forgesync

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// Server accepts one sync at a time on a connection. The node keeps its own log.
type Server struct {
	ln net.Listener
	n  *Node
	wg sync.WaitGroup
}

// Listen serves Sync sessions at addr. The returned server is already accepting.
func (n *Node) Listen(addr string) (*Server, error) {
	if n == nil {
		return nil, ErrInvalid
	}
	if addr == "" {
		return nil, errors.New("address is required")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{ln: ln, n: n}
	s.wg.Add(1)
	go s.accept()
	return s, nil
}

func (s *Server) Addr() string { return s.ln.Addr().String() }

func (s *Server) Close() error {
	err := s.ln.Close()
	s.wg.Wait()
	return err
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			_ = session(s.n, conn, false)
		}()
	}
}

// Dial connects to a listening node and exchanges changes until both sides are quiet.
func Dial(addr string, n *Node) error {
	if n == nil {
		return ErrInvalid
	}
	if addr == "" {
		return errors.New("address is required")
	}
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).Dial("tcp", addr)
	if err != nil {
		return err
	}
	return session(n, conn, true)
}

type envelope struct {
	Type    string   `json:"type"`
	ID      string   `json:"id,omitempty"`
	Cursor  Cursor   `json:"cursor,omitempty"`
	Changes []Change `json:"changes,omitempty"`
}

func session(n *Node, conn net.Conn, dialer bool) error {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	var peer Cursor
	if dialer {
		if err := writeEnv(rw, envelope{Type: "hello", ID: n.ID(), Cursor: mustCursor(n)}); err != nil {
			return err
		}
		env, err := readEnv(rw)
		if err != nil {
			return err
		}
		if env.Type != "hello" {
			return ErrInvalid
		}
		peer = env.Cursor
	} else {
		env, err := readEnv(rw)
		if err != nil {
			return err
		}
		if env.Type != "hello" {
			return ErrInvalid
		}
		peer = env.Cursor
		if err := writeEnv(rw, envelope{Type: "hello", ID: n.ID(), Cursor: mustCursor(n)}); err != nil {
			return err
		}
	}
	if peer == nil {
		peer = Cursor{}
	}
	for i := 0; i < 16; i++ {
		if dialer {
			out, err := takeMissing(n, peer)
			if err != nil {
				return err
			}
			if err := writeEnv(rw, envelope{Type: "changes", Changes: out}); err != nil {
				return err
			}
			env, err := readEnv(rw)
			if err != nil {
				return err
			}
			if env.Type != "changes" {
				return ErrInvalid
			}
			if err := n.Apply(env.Changes); err != nil {
				return err
			}
			bump(peer, out)
			if len(out) == 0 && len(env.Changes) == 0 {
				return nil
			}
			continue
		}
		env, err := readEnv(rw)
		if err != nil {
			return err
		}
		if env.Type != "changes" {
			return ErrInvalid
		}
		if err := n.Apply(env.Changes); err != nil {
			return err
		}
		bump(peer, env.Changes)
		out, err := takeMissing(n, peer)
		if err != nil {
			return err
		}
		if err := writeEnv(rw, envelope{Type: "changes", Changes: out}); err != nil {
			return err
		}
		bump(peer, out)
		if len(out) == 0 && len(env.Changes) == 0 {
			return nil
		}
	}
	return errors.New("sync did not settle")
}

func takeMissing(n *Node, peer Cursor) ([]Change, error) {
	out, err := n.Missing(peer)
	if err != nil {
		return nil, err
	}
	if len(out) > maxBatch {
		out = out[:maxBatch]
	}
	return out, nil
}

func mustCursor(n *Node) Cursor {
	c, err := n.Cursor()
	if err != nil || c == nil {
		return Cursor{}
	}
	return c
}

func writeEnv(rw *bufio.ReadWriter, env envelope) error {
	if env.Changes == nil {
		env.Changes = []Change{}
	}
	if env.Cursor == nil {
		env.Cursor = Cursor{}
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if len(body) > maxFrame {
		return ErrTooLarge
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := rw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := rw.Write(body); err != nil {
		return err
	}
	return rw.Flush()
}

func readEnv(rw *bufio.ReadWriter) (envelope, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(rw, hdr[:]); err != nil {
		return envelope{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrame {
		return envelope{}, ErrTooLarge
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(rw, buf); err != nil {
		return envelope{}, err
	}
	var env envelope
	if err := json.Unmarshal(buf, &env); err != nil {
		return envelope{}, err
	}
	if env.Cursor == nil {
		env.Cursor = Cursor{}
	}
	if env.Changes == nil {
		env.Changes = []Change{}
	}
	return env, nil
}
