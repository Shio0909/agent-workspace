package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

// DefaultMaxOperations 是幂等记录的保留上限。超过之后按完成时间淘汰最旧的
// 终态记录：幂等保证因此是有时间边界的，不是永久保证。
const DefaultMaxOperations = 10000

const (
	stateFile   = "state.db"
	schemaValue = "1"
)

var (
	workspacesBucket = []byte("workspaces")
	operationsBucket = []byte("operations")
	metaBucket       = []byte("meta")
	schemaKey        = []byte("schema")
)

// keyStripes serialises writes of one key without serialising the whole store.
// Two writes of the same workspace must reach disk and memory in the same
// order; writes of different workspaces may share one group commit.
const keyStripes = 64

// Store 是本地持久化状态，被刻意限制为单控制器：目录上的建议锁会拒绝第二个
// 进程。工作区、幂等记录和审计日志都放在同一个独占目录里，因为它们共享同一
// 份"谁在写这个数据目录"的独占性，也共享同一个生命周期。
//
// 持久层是 bbolt，每次写入只写一条记录；内存里保留一份完整副本服务读请求。
// 内存副本只在写入落盘之后才更新，所以读者永远看不到没有持久化的状态。
type Store struct {
	mu   sync.Mutex
	db   *bolt.DB
	lock *os.File
	// commits 合并并发写入：同一时刻排队的写入共用一次事务和一次 fsync。
	commits *groupCommit
	keys    [keyStripes]sync.Mutex
	// opsMu 覆盖幂等记录的"检查 + 落盘 + 更新内存"整个过程：插入是 biz_id
	// 上的事务边界，不能在落盘期间让第二个同 biz_id 的提交看到"不存在"。
	opsMu sync.Mutex
	items map[string]Workspace
	ops   map[string]Operation
	// order 是 items 的有序索引缓存，新增 ID 之后失效。分页扫描因此每轮只排序
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
	s := &Store{lock: f, MaxOperations: DefaultMaxOperations}
	s.db, err = bolt.Open(filepath.Join(dir, stateFile), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		err = fmt.Errorf("open state database: %w", err)
	}
	if err == nil {
		s.commits = &groupCommit{db: s.db}
		err = s.migrate(dir)
	}
	if err == nil {
		err = s.load()
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

// migrate imports the JSON snapshots written by earlier versions, once. The
// schema marker, not an empty bucket, decides whether the import already
// happened: an empty store is a valid state and must not pick up a stale file.
func (s *Store) migrate(dir string) error {
	legacy := []string{filepath.Join(dir, "workspaces.json"), filepath.Join(dir, "operations.json")}
	imported := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}
		for _, name := range [][]byte{workspacesBucket, operationsBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		if meta.Get(schemaKey) != nil {
			return nil
		}
		items, err := loadSnapshot[Workspace](legacy[0])
		if err != nil {
			return fmt.Errorf("read workspace snapshot: %w", err)
		}
		ops, err := loadSnapshot[Operation](legacy[1])
		if err != nil {
			return fmt.Errorf("read operation snapshot: %w", err)
		}
		if err := putAll(tx.Bucket(workspacesBucket), items); err != nil {
			return err
		}
		if err := putAll(tx.Bucket(operationsBucket), ops); err != nil {
			return err
		}
		imported = len(items)+len(ops) > 0
		return meta.Put(schemaKey, []byte(schemaValue))
	})
	if err != nil || !imported {
		return err
	}
	// The originals are kept, renamed, so a rollback to the JSON version still
	// has its data. A crash before the rename is harmless: the schema marker is
	// already committed and the files are ignored from now on.
	for _, path := range legacy {
		if err := os.Rename(path, path+".migrated"); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("keep migrated snapshot", "path", path, "error", err)
		}
	}
	slog.Info("imported JSON snapshot into state database")
	return nil
}

func putAll[T any](b *bolt.Bucket, items map[string]T) error {
	for id, item := range items {
		v, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(id), v); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) load() error {
	return s.db.View(func(tx *bolt.Tx) error {
		var err error
		if s.items, err = loadBucket[Workspace](tx.Bucket(workspacesBucket)); err != nil {
			return fmt.Errorf("read workspaces: %w", err)
		}
		if s.ops, err = loadBucket[Operation](tx.Bucket(operationsBucket)); err != nil {
			return fmt.Errorf("read operations: %w", err)
		}
		return nil
	})
}

func loadBucket[T any](b *bolt.Bucket) (map[string]T, error) {
	out := map[string]T{}
	err := b.ForEach(func(k, v []byte) error {
		var item T
		if err := json.Unmarshal(v, &item); err != nil {
			return fmt.Errorf("record %q: %w", k, err)
		}
		out[string(k)] = item
		return nil
	})
	return out, err
}

func (s *Store) Close() error {
	if s.audit != nil {
		_ = s.audit.Close()
	}
	var err error
	if s.db != nil {
		err = s.db.Close()
	}
	return errors.Join(err, s.lock.Close())
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
// 分批推进长扫描：一轮里同时展开的工作区数量由页大小决定，而不是由总量决定。
// 读的是内存副本，分页限制的是工作集与并发，不是磁盘 IO。
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

func (s *Store) keyLock(id string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return &s.keys[h.Sum32()%keyStripes]
}

// Put 持久化一个工作区。写入成本只和这一条记录有关，与工作区总数无关。
func (s *Store) Put(w Workspace) error {
	w = clone(w)
	v, err := json.Marshal(w)
	if err != nil {
		return err
	}
	l := s.keyLock(w.ID)
	l.Lock()
	defer l.Unlock()
	if err := s.commits.do(func(tx *bolt.Tx) error {
		return tx.Bucket(workspacesBucket).Put([]byte(w.ID), v)
	}); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 只有 ID 集合变化才会让有序索引过期。对账本身会频繁改写已有工作区，
	// 如果每次写入都重排，分页扫描就退化成每页排一次全量。
	if _, existed := s.items[w.ID]; !existed {
		s.order = nil
	}
	s.items[w.ID] = w
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
	s.opsMu.Lock()
	defer s.opsMu.Unlock()
	s.mu.Lock()
	existing, ok := s.ops[op.BizID]
	var victims []string
	if !ok {
		victims = evictOperations(s.ops, s.MaxOperations)
	}
	s.mu.Unlock()
	if ok {
		return existing, true, nil
	}
	v, err := json.Marshal(op)
	if err != nil {
		return Operation{}, false, err
	}
	if err := s.commits.do(func(tx *bolt.Tx) error {
		b := tx.Bucket(operationsBucket)
		for _, id := range victims {
			if err := b.Delete([]byte(id)); err != nil {
				return err
			}
		}
		return b.Put([]byte(op.BizID), v)
	}); err != nil {
		return Operation{}, false, err
	}
	if len(victims) > 0 {
		slog.Debug("evicted finished operations", "count", len(victims), "max", s.MaxOperations)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range victims {
		delete(s.ops, id)
	}
	s.ops[op.BizID] = op
	return op, false, nil
}

func (s *Store) putOperation(op Operation) error {
	s.opsMu.Lock()
	defer s.opsMu.Unlock()
	s.mu.Lock()
	_, ok := s.ops[op.BizID]
	s.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	v, err := json.Marshal(op)
	if err != nil {
		return err
	}
	if err := s.commits.do(func(tx *bolt.Tx) error {
		return tx.Bucket(operationsBucket).Put([]byte(op.BizID), v)
	}); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops[op.BizID] = op
	return nil
}

// evictOperations 选出插入 incoming 之后需要淘汰的记录，不修改 ops。只淘汰
// 终态记录，按完成时间从旧到新。processing 记录永不淘汰：淘汰一条正在执行的
// 记录，等于允许同一个操作被再执行一次。如果所有记录都还在执行中，就允许
// 超限而不误伤。
func evictOperations(ops map[string]Operation, max int) []string {
	excess := len(ops) + 1 - max
	if max <= 0 || excess <= 0 {
		return nil
	}
	type candidate struct {
		id string
		at time.Time
	}
	var finished []candidate
	for id, op := range ops {
		if op.Status == OpProcessing {
			continue
		}
		at := op.FinishedAt
		if at.IsZero() {
			at = op.StartedAt
		}
		finished = append(finished, candidate{id, at})
	}
	// The incoming record counts towards the limit but is never a candidate:
	// evicting it would turn the insert into a silent no-op.
	excess = min(excess, len(finished))
	sort.Slice(finished, func(i, j int) bool { return finished[i].at.Before(finished[j].at) })
	out := make([]string, 0, excess)
	for _, c := range finished[:excess] {
		out = append(out, c.id)
	}
	return out
}

// groupCommit batches concurrent writes into one bbolt transaction, so N
// writers waiting at the same time pay for one fsync instead of N. Unlike
// bolt.DB.Batch it adds no timer: a lone writer commits immediately, and a
// batch is simply whatever queued while the previous commit was on disk.
type groupCommit struct {
	db      *bolt.DB
	mu      sync.Mutex
	queue   []*commitReq
	leading bool
}

type commitReq struct {
	apply func(*bolt.Tx) error
	done  chan commitResult
}

type commitResult struct {
	err  error
	lead bool
}

func (g *groupCommit) do(apply func(*bolt.Tx) error) error {
	req := &commitReq{apply: apply, done: make(chan commitResult, 1)}
	g.mu.Lock()
	g.queue = append(g.queue, req)
	if !g.leading {
		g.leading = true
		g.mu.Unlock()
		return g.lead(req)
	}
	g.mu.Unlock()
	if r := <-req.done; !r.lead {
		return r.err
	}
	return g.lead(req)
}

// lead commits everything queued so far, then hands leadership to the oldest
// waiter instead of looping. Looping would keep one caller committing other
// callers' writes for as long as load lasts.
func (g *groupCommit) lead(own *commitReq) error {
	g.mu.Lock()
	batch := g.queue
	g.queue = nil
	g.mu.Unlock()

	err := g.db.Update(func(tx *bolt.Tx) error {
		for _, r := range batch {
			if err := r.apply(tx); err != nil {
				return err
			}
		}
		return nil
	})
	results := make([]error, len(batch))
	if err != nil && len(batch) > 1 {
		// One bad write must not fail the others that happened to share its
		// transaction, so retry them one by one.
		for i, r := range batch {
			results[i] = g.db.Update(r.apply)
		}
	} else {
		for i := range batch {
			results[i] = err
		}
	}
	var ownErr error
	for i, r := range batch {
		if r == own {
			ownErr = results[i]
			continue
		}
		r.done <- commitResult{err: results[i]}
	}

	g.mu.Lock()
	if len(g.queue) == 0 {
		g.leading = false
		g.mu.Unlock()
		return ownErr
	}
	next := g.queue[0]
	g.mu.Unlock()
	next.done <- commitResult{lead: true}
	return ownErr
}

// loadSnapshot reads a legacy JSON snapshot. It is used only by migrate.
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
