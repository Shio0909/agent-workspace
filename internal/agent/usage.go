package agent

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Usage is the cumulative token consumption of this workspace since it was
// created. It is reported on /heartbeat and persisted on the volume, so a pod
// restart does not reset it.
type Usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

// usageMeter keeps the running total. Every model call goes through add,
// including summarisation calls, which cost tokens like any other.
type usageMeter struct {
	mu    sync.Mutex
	path  string
	total Usage
}

// usagePath is outside files/, the only directory the model's tools can write
// to, so a prompt cannot lower its own counter.
func usagePath(workspaceDir string) string {
	if workspaceDir == "" {
		return ""
	}
	return filepath.Join(workspaceDir, "usage.json")
}

// loadUsage restores the total. A missing file is a new workspace. An
// unreadable one restarts from zero: the controller keeps the largest value it
// has seen, so this under-reports until the counter catches up instead of
// failing the agent.
func loadUsage(path string) *usageMeter {
	m := &usageMeter{path: path}
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("agent: read %s: %v", path, err)
		}
		return m
	}
	var u Usage
	if err := json.Unmarshal(b, &u); err != nil || u.PromptTokens < 0 || u.CompletionTokens < 0 || u.TotalTokens < 0 {
		log.Printf("agent: ignoring unreadable usage file %s", path)
		return m
	}
	m.total = u
	return m
}

func (m *usageMeter) snapshot() Usage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.total
}

// add counts one provider-reported usage object and persists the new total
// before returning, so a crash loses at most the call in flight. A response
// without usage adds nothing: guessing from text length would be a number
// nobody could check.
func (m *usageMeter) add(u Usage) {
	if u.PromptTokens < 0 || u.CompletionTokens < 0 || u.TotalTokens < 0 {
		return
	}
	// Some providers omit total_tokens, and some count more than the two
	// parts (reasoning tokens); never report less than the parts.
	u.TotalTokens = max(u.TotalTokens, u.PromptTokens+u.CompletionTokens)
	if u == (Usage{}) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total.PromptTokens += u.PromptTokens
	m.total.CompletionTokens += u.CompletionTokens
	m.total.TotalTokens += u.TotalTokens
	if err := m.save(); err != nil {
		log.Printf("agent: persist usage: %v", err)
	}
}

// save replaces the file atomically, the way saveSummary does: a crash leaves
// the previous total.
func (m *usageMeter) save() error {
	if m.path == "" {
		return nil
	}
	dir := filepath.Dir(m.path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	b, err := json.Marshal(m.total)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "usage.*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), m.path)
}
