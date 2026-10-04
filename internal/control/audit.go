package control

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AuditEvent 是一条只追加的审计记录。Actor 只是归属信息：控制面只校验一个
// 共享令牌，调用方可以声称任意身份，所以它不能作为授权或追责依据。
type AuditEvent struct {
	At        time.Time `json:"at"`
	Actor     string    `json:"actor"`
	Action    string    `json:"action"`
	Workspace string    `json:"workspace,omitempty"`
	Detail    string    `json:"detail,omitempty"`
	Result    string    `json:"result"`
}

const (
	ResultOK    = "ok"
	ResultError = "error"
)

// AuditQuery 按工作区、动作和时间范围过滤。Since 是闭区间、Until 是开区间，
// 这样相邻两段查询不会重复计入同一毫秒的边界事件。
type AuditQuery struct {
	Workspace string
	Action    string
	Since     time.Time
	Until     time.Time
	// Limit 按追加顺序取前 N 条；0 表示不限。想要最近 N 条请显式给 Since，
	// 因为只追加的文件不会倒着读。
	Limit int
}

// AuditLog 保留当前 JSONL 文件和有限归档。查询先打开文件快照，再解锁扫描；
// 即使扫描期间轮转、删除旧归档，打开的文件仍可读取。
const (
	defaultAuditMaxBytes = 8 << 20
	defaultAuditArchives = 4
	maxAuditRecordBytes  = 1 << 20
)

type AuditLog struct {
	mu         sync.Mutex
	path       string
	file       *os.File
	size       int64
	sequence   uint64
	maxBytes   int64
	archives   int
	closed     bool
	needsPrune bool
}

func OpenAuditLog(path string) (*AuditLog, error) {
	return openAuditLog(path, defaultAuditMaxBytes, defaultAuditArchives)
}

func openAuditLog(path string, maxBytes int64, archives int) (*AuditLog, error) {
	path = filepath.Clean(path)
	l := &AuditLog{path: path, maxBytes: maxBytes, archives: archives}
	files, err := l.archivePaths()
	if err != nil {
		return nil, err
	}
	l.needsPrune = len(files) > archives
	if len(files) > 0 {
		l.sequence, _ = strconv.ParseUint(strings.TrimPrefix(files[len(files)-1], path+"."), 10, 64)
	}
	if err := l.openActive(); err != nil {
		return nil, err
	}
	return l, nil
}

// openActive 也恢复“旧文件已归档、新文件还没创建”时中断的轮转。
func (l *AuditLog) openActive() error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if err := repairTornTail(f); err != nil {
		f.Close()
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	l.file, l.size = f, info.Size()
	return nil
}

// 只识别本日志的固定宽度序号，忽略同目录的其他文件。
func (l *AuditLog) archivePaths() ([]string, error) {
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		return nil, err
	}
	prefix := filepath.Base(l.path) + "."
	var paths []string
	for _, entry := range entries {
		suffix, ok := strings.CutPrefix(entry.Name(), prefix)
		if entry.IsDir() || !ok || len(suffix) != 20 || strings.Trim(suffix, "0123456789") != "" {
			continue
		}
		if _, err := strconv.ParseUint(suffix, 10, 64); err != nil {
			continue
		}
		paths = append(paths, filepath.Join(filepath.Dir(l.path), entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

// rotate 不搬动既有归档，避免中断一串重命名导致历史被覆盖。
func (l *AuditLog) rotate() error {
	archive := fmt.Sprintf("%s.%020d", l.path, l.sequence+1)
	if err := os.Rename(l.path, archive); err != nil {
		return err
	}
	l.sequence++
	l.needsPrune = true
	err := l.file.Close()
	l.file = nil
	if err != nil {
		return err
	}
	return l.openActive()
}

func (l *AuditLog) prune() error {
	paths, err := l.archivePaths()
	if err != nil {
		return err
	}
	for _, path := range paths[:max(0, len(paths)-l.archives)] {
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

// repairTornTail 在文件末尾补一个换行。崩溃可能把一条记录只写了一半，如果不
// 补，下一次追加会直接接在半行后面，把新记录也一起写坏。补一行的代价是只丢
// 半行那一条，而不是丢掉后面所有记录。
func repairTornTail(f *os.File) error {
	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return err
	}
	last := make([]byte, 1)
	if _, err := f.ReadAt(last, info.Size()-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	_, err = f.Write([]byte("\n"))
	return err
}

func (l *AuditLog) Path() string { return l.path }

func (l *AuditLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.file == nil {
		return nil
	}
	return l.file.Close()
}

// Append 仍同步 fsync；单条记录不拆分，过大的记录拒绝，避免无法查询。
func (l *AuditLog) Append(e AuditEvent) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if len(b) > maxAuditRecordBytes {
		return fmt.Errorf("audit record exceeds %d bytes", maxAuditRecordBytes)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return os.ErrClosed
	}
	if l.file == nil {
		if err := l.openActive(); err != nil {
			return err
		}
	}
	if l.size > 0 && l.size+int64(len(b)) > l.maxBytes {
		if err := l.rotate(); err != nil {
			return err
		}
	}
	n, err := l.file.Write(b)
	l.size += int64(n)
	if err != nil {
		return err
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	// 新记录落盘后才淘汰历史。失败由既有 AuditFailures 机制暴露。
	if l.needsPrune {
		if err := l.prune(); err != nil {
			return err
		}
		l.needsPrune = false
	}
	return nil
}

type auditFile struct {
	file *os.File
	size int64
}

func closeAuditFiles(files []auditFile) {
	for _, f := range files {
		f.file.Close()
	}
}

func (l *AuditLog) snapshot() ([]auditFile, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	paths, err := l.archivePaths()
	if err != nil {
		return nil, err
	}
	paths = append(paths, l.path)
	var files []auditFile
	for _, path := range paths {
		f, err := os.Open(path)
		if errors.Is(err, os.ErrNotExist) && path == l.path {
			continue
		}
		if err != nil {
			closeAuditFiles(files)
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			closeAuditFiles(files)
			return nil, err
		}
		files = append(files, auditFile{f, info.Size()})
	}
	return files, nil
}

// Query 按追加顺序扫描保留的历史；快照后的追加不会改变本次查询。
func (l *AuditLog) Query(q AuditQuery) ([]AuditEvent, error) {
	files, err := l.snapshot()
	if err != nil {
		return nil, err
	}
	defer closeAuditFiles(files)
	return readAuditFiles(files, q)
}

func readAuditFiles(files []auditFile, q AuditQuery) ([]AuditEvent, error) {
	var out []AuditEvent
	for _, f := range files {
		scanner := bufio.NewScanner(io.NewSectionReader(f.file, 0, f.size))
		scanner.Buffer(make([]byte, 0, 16*1024), maxAuditRecordBytes+1)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			var e AuditEvent
			// 崩溃留下的半行不影响后续完整记录。
			if err := json.Unmarshal(line, &e); err != nil || !q.match(e) {
				continue
			}
			out = append(out, e)
			if q.Limit > 0 && len(out) >= q.Limit {
				return out, nil
			}
		}
		if err := scanner.Err(); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (q AuditQuery) match(e AuditEvent) bool {
	if q.Workspace != "" && e.Workspace != q.Workspace {
		return false
	}
	if q.Action != "" && e.Action != q.Action {
		return false
	}
	if !q.Since.IsZero() && e.At.Before(q.Since) {
		return false
	}
	if !q.Until.IsZero() && !e.At.Before(q.Until) {
		return false
	}
	return true
}
