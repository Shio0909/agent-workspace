package control

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
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
	// Limit 按时间正序取前 N 条；0 表示不限。想要最近 N 条请显式给 Since，
	// 因为只追加的文件不会倒着读。
	Limit int
}

// AuditLog 是 append-only 的 JSONL 文件。选 JSONL 而不是结构化存储，是为了
// 让审计在控制器之外也能被读取：一次 grep 就能查，不需要本项目的代码。
type AuditLog struct {
	mu   sync.Mutex
	path string
	file *os.File
}

func OpenAuditLog(path string) (*AuditLog, error) {
	// O_RDWR 而不是只写：打开时要读最后一个字节，判断上次是否留下了半行。
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	if err := repairTornTail(f); err != nil {
		f.Close()
		return nil, err
	}
	return &AuditLog{path: path, file: f}, nil
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
	return l.file.Close()
}

// Append 写入一行并 fsync。审计是同步写：调用方宁可多等一次磁盘，也不希望
// 崩溃后查不到刚刚发生的状态变更。
func (l *AuditLog) Append(e AuditEvent) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.file.Write(b); err != nil {
		return err
	}
	return l.file.Sync()
}

// Query 每次都重新读文件。审计日志是进程重启后唯一的事实来源，所以这里不
// 缓存、也不维护内存副本，代价是查询比状态快照慢，这是有意的取舍。
func (l *AuditLog) Query(q AuditQuery) ([]AuditEvent, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.Open(l.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// 单条事件可能带着较长的错误详情，默认 64KiB 的行上限太小。
	scanner.Buffer(make([]byte, 0, 16*1024), 1<<20)
	var out []AuditEvent
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var e AuditEvent
		// 崩溃可能留下写了一半的行；跳过它，而不是让整个查询失败。
		if err := json.Unmarshal(line, &e); err != nil {
			continue
		}
		if !q.match(e) {
			continue
		}
		out = append(out, e)
		if q.Limit > 0 && len(out) >= q.Limit {
			break
		}
	}
	return out, scanner.Err()
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
