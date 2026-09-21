package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

// DefaultMaxOperations 是幂等记录的保留上限。超过之后按完成时间淘汰最旧的
// 终态记录：幂等保证因此是有时间边界的，不是永久保证。
const DefaultMaxOperations = 10000

// Store 是一个原子的本地快照，被刻意限制为单控制器：目录上的建议锁会拒绝
// 第二个进程。工作区快照、幂等记录和审计日志都放在同一个独占目录里，因为
// 它们共享同一份"谁在写这个数据目录"的独占性，也共享同一个生命周期。
type Store struct {
	mu      sync.Mutex
	path    string
	opsPath string
	lock    *os.File
	items   map[string]Workspace
	ops     map[string]Operation
	// order 是 items 的有序索引缓存，Put 之后失效。分页扫描因此每轮只排序
	// 一次，而不是每翻一页重排一次。
	order         []string
	audit         *AuditLog
	MaxOperations int
}

func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "controller.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data directory is already in use: %w", err)
	}
	s := &Store{path: filepath.Join(dir, "workspaces.json"), opsPath: filepath.Join(dir, "operations.json"),
		lock: f, MaxOperations: DefaultMaxOperations}
	s.items, err = loadSnapshot[Workspace](s.path)
	if err != nil {
		err = fmt.Errorf("read workspace snapshot: %w", err)
	}
	if err == nil {
		s.ops, err = loadSnapshot[Operation](s.opsPath)
		if err != nil {
			err = fmt.Errorf("read operation snapshot: %w", err)
		}
	}
	if err == nil {
		// 审计日志只追加、从不整体解析，所以写坏的一行不会阻止控制器启动。
		s.audit, err = OpenAuditLog(filepath.Join(dir, "audit.jsonl"))
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s.audit != nil {
		_ = s.audit.Close()
	}
	return s.lock.Close()
}

// Audit 返回数据目录里的审计日志。OpenStore 成功时它一定非 nil。
func (s *Store) Audit() *AuditLog { return s.audit }

func clone(w Workspace) Workspace {
	leases := make(map[string]time.Time, len(w.Leases))
	for k, v := range w.Leases {
		leases[k] = v
	}
	w.Leases = leases
	return w
}

func (s *Store) Get(id string) (Workspace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.items[id]
	if !ok {
		return Workspace{}, ErrNotFound
	}
	return clone(w), nil
}

func (s *Store) List() []Workspace {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Workspace, 0, len(s.items))
	for _, w := range s.items {
		out = append(out, clone(w))
	}
	return out
}

// Scan 以 ID 为游标返回一页工作区，more 表示游标之后还有数据。控制器用它
// 分批推进长扫描：一轮里同时展开的工作区数量由页大小决定，而不是由快照
// 总量决定。注意 Store 本身是内存快照，分页限制的是工作集与并发，不是磁盘 IO。
func (s *Store) Scan(after string, limit int) ([]Workspace, bool) {
	if limit <= 0 {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.sortedIDs()
	start := sort.SearchStrings(ids, after)
	if start < len(ids) && ids[start] == after {
		start++
	}
	end := start + limit
	if end > len(ids) {
		end = len(ids)
	}
	page := make([]Workspace, 0, end-start)
	for _, id := range ids[start:end] {
		page = append(page, clone(s.items[id]))
	}
	return page, end < len(ids)
}

// sortedIDs 在锁内使用，返回按 ID 排序的索引。调用方必须持有 s.mu。
func (s *Store) sortedIDs() []string {
	if s.order == nil {
		s.order = make([]string, 0, len(s.items))
		for id := range s.items {
			s.order = append(s.order, id)
		}
		sort.Strings(s.order)
	}
	return s.order
}

func (s *Store) Put(w Workspace) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]Workspace, len(s.items)+1)
	for id, item := range s.items {
		next[id] = item
	}
	_, existed := s.items[w.ID]
	next[w.ID] = clone(w)
	if err := saveSnapshot(s.path, next); err != nil {
		return err
	}
	s.items = next
	// 只有 ID 集合变化才会让有序索引过期。对账本身会频繁改写已有工作区，
	// 如果每次写入都重排，分页扫描就退化成每页排一次全量。
	if !existed {
		s.order = nil
	}
	return nil
}

func (s *Store) GetOperation(bizID string) (Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[bizID]
	if !ok {
		return Operation{}, ErrNotFound
	}
	return op, nil
}

func (s *Store) ListOperations() []Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Operation, 0, len(s.ops))
	for _, op := range s.ops {
		out = append(out, op)
	}
	return out
}

// putOperationIfAbsent 是幂等记录的插入点，也是 biz_id 上的事务边界：检查与
// 写入在同一把锁内完成，所以并发提交同一个 biz_id 只会有一次插入成功。返回
// 的 bool 表示记录本来就在（此时返回已存记录）。
func (s *Store) putOperationIfAbsent(op Operation) (Operation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.ops[op.BizID]; ok {
		return existing, true, nil
	}
	next := make(map[string]Operation, len(s.ops)+1)
	for k, v := range s.ops {
		next[k] = v
	}
	next[op.BizID] = op
	if evicted := evictOperations(next, s.MaxOperations); evicted > 0 {
		slog.Debug("evicted finished operations", "count", evicted, "max", s.MaxOperations)
	}
	if err := saveSnapshot(s.opsPath, next); err != nil {
		return Operation{}, false, err
	}
	s.ops = next
	return op, false, nil
}

func (s *Store) putOperation(op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ops[op.BizID]; !ok {
		return ErrNotFound
	}
	next := make(map[string]Operation, len(s.ops))
	for k, v := range s.ops {
		next[k] = v
	}
	next[op.BizID] = op
	if err := saveSnapshot(s.opsPath, next); err != nil {
		return err
	}
	s.ops = next
	return nil
}

// evictOperations 只淘汰终态记录，按完成时间从旧到新。processing 记录永不
// 淘汰：淘汰一条正在执行的记录，等于允许同一个操作被再执行一次。如果所有
// 记录都还在执行中，就允许超限而不误伤。
func evictOperations(ops map[string]Operation, max int) int {
	if max <= 0 {
		return 0
	}
	evicted := 0
	for len(ops) > max {
		victim := ""
		var oldest time.Time
		for id, op := range ops {
			if op.Status == OpProcessing {
				continue
			}
			at := op.FinishedAt
			if at.IsZero() {
				at = op.StartedAt
			}
			if victim == "" || at.Before(oldest) {
				victim, oldest = id, at
			}
		}
		if victim == "" {
			return evicted
		}
		delete(ops, victim)
		evicted++
	}
	return evicted
}

func loadSnapshot[T any](path string) (map[string]T, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]T{}, nil
	}
	if err != nil {
		return nil, err
	}
	var items map[string]T
	if err := json.Unmarshal(b, &items); err != nil {
		return nil, err
	}
	if items == nil {
		return nil, errors.New("invalid null snapshot")
	}
	return items, nil
}

// saveSnapshot 先写临时文件再 rename：读者要么看到旧快照，要么看到新快照，
// 不会看到写了一半的文件。
func saveSnapshot[T any](path string, items map[string]T) error {
	b, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
