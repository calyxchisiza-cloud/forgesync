package forgesync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const logName = "changes.log"

// Open loads a node from dir, or creates the directory. The id is stored
// beside the log. Reopening with a different id fails.
func Open(dir, id string) (*Node, error) {
	if err := validID(id); err != nil {
		return nil, err
	}
	if dir == "" {
		return nil, fmt.Errorf("%w: directory is required", ErrInvalid)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	idPath := filepath.Join(dir, "id")
	existing, err := os.ReadFile(idPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(idPath, []byte(id), 0o644); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if string(existing) != id {
		return nil, fmt.Errorf("%w: directory belongs to %s", ErrInvalid, existing)
	}
	n := newNode(id)
	if err := n.openLog(filepath.Join(dir, logName)); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) openLog(path string) error {
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		if cut := bytes.LastIndexByte(data, '\n'); cut >= 0 {
			data = data[:cut+1]
		} else {
			data = nil
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	if err := n.replay(data); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	n.file = f
	return nil
}

func (n *Node) replay(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var c Change
		if err := json.Unmarshal(line, &c); err != nil {
			return fmt.Errorf("change log: %w", err)
		}
		if err := n.applyLocked(c); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) append(c Change) error {
	if n.file == nil {
		return nil
	}
	body, err := json.Marshal(c)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	if _, err := n.file.Write(body); err != nil {
		return err
	}
	return n.file.Sync()
}
