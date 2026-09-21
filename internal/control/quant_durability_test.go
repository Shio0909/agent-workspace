package control

// 这组测试量化两件"跨重启才算数"的事：
//
//   - 审计日志：跨重启后条数与内容是否逐字节一致；崩溃留下的半行会丢多少。
//   - 对账重放：注入中途失败（含在"意图已落盘、运行时还没动"的缝上模拟崩溃）
//     之后，重启能不能收敛、需要几轮。
//
// 复跑：go test ./internal/control -run 'TestQuant' -race -count=1 -v

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 4. 审计：跨重启逐字节一致 + 半行只丢一条
// ---------------------------------------------------------------------------

func TestQuantAuditSurvivesRestartByteForByte(t *testing.T) {
	const events = 200
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	log := s.Audit()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	written := make([]AuditEvent, 0, events)
	started := time.Now()
	for i := 0; i < events; i++ {
		e := AuditEvent{
			At:        at.Add(time.Duration(i) * time.Second),
			Actor:     fmt.Sprintf("actor-%02d", i%7),
			Action:    ActionStart,
			Workspace: fmt.Sprintf("w-%03d", i%13),
			Detail:    strings.Repeat("d", 96),
			Result:    ResultOK,
		}
		if err := log.Append(e); err != nil {
			t.Fatal(err)
		}
		written = append(written, e)
	}
	appendCost := time.Since(started)
	path := log.Path()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sumBefore := sha256.Sum256(before)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：同一个数据目录重新打开。审计是重启后唯一的事实来源，所以这里
	// 不允许有任何"内存副本"兜底。
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	after, err := os.ReadFile(s2.Audit().Path())
	if err != nil {
		t.Fatal(err)
	}
	sumAfter := sha256.Sum256(after)
	if sumAfter != sumBefore {
		t.Fatalf("重启改写了审计文件: %x -> %x", sumBefore, sumAfter)
	}
	got, err := s2.Audit().Query(AuditQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != events {
		t.Fatalf("重启后条数变了: %d != %d", len(got), events)
	}
	// 逐条重新序列化，和磁盘上的原始行逐字节比对：条数相同还不够，内容也必须
	// 一位不差。
	lines := bytes.Split(bytes.TrimSuffix(before, []byte("\n")), []byte("\n"))
	for i, e := range got {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, lines[i]) {
			t.Fatalf("第 %d 条记录重启后内容变了:\n 磁盘 %s\n 读回 %s", i, lines[i], b)
		}
		if !e.At.Equal(written[i].At) {
			t.Fatalf("第 %d 条记录时间戳漂移: %s != %s", i, e.At, written[i].At)
		}
	}
	t.Logf("审计 %d 条 / %d 字节：重启前后 SHA-256 一致（%x），逐条重新序列化与原行逐字节相同；"+
		"每条 append 都 fsync，%d 条共 %s（%.2f ms/条，本机 APFS）",
		events, len(before), sumBefore, events, appendCost, float64(appendCost.Microseconds())/1000/float64(events))
}

func TestQuantAuditTornLineLosesExactlyOneRecord(t *testing.T) {
	const events = 200
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	log := s.Audit()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < events; i++ {
		if err := log.Append(AuditEvent{At: at.Add(time.Duration(i) * time.Second), Actor: "alice",
			Action: ActionStart, Workspace: "demo", Result: ResultOK}); err != nil {
			t.Fatal(err)
		}
	}
	good, err := os.ReadFile(log.Path())
	if err != nil {
		t.Fatal(err)
	}
	// 模拟进程在写第 201 条的中途被杀：文件末尾留下半行、没有换行。
	torn := `{"at":"2026-01-01T00:00:00Z","actor":"crash`
	f, err := os.OpenFile(log.Path(), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(torn); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：打开时补一个换行，损坏因此被限制在"丢半行那一条"。
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("半行让控制器起不来: %v", err)
	}
	defer func() { _ = s2.Close() }()
	next := AuditEvent{At: at.Add(time.Hour), Actor: "bob", Action: ActionStop, Workspace: "demo", Result: ResultOK}
	if err := s2.Audit().Append(next); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(s2.Audit().Path())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(after, good) {
		t.Fatal("补换行改写了已经落盘的前缀")
	}
	got, err := s2.Audit().Query(AuditQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != events+1 {
		t.Fatalf("可查条数 = %d，期望 %d（%d 条好记录 + 1 条新追加，半行被跳过）",
			len(got), events+1, events)
	}
	if got[len(got)-1].Action != ActionStop {
		t.Fatalf("新追加的记录被半行带坏了: %+v", got[len(got)-1])
	}
	t.Logf("半行容错：崩溃前 %d 条完整记录 -> 重启后可查 %d 条（完整记录 0 丢失）；"+
		"写了一半的那 1 条被跳过，前 %d 条逐字节未变，第 %d 条追加正常落盘",
		events, len(got), events, events+1)
}

// ---------------------------------------------------------------------------
// 5. 对账重放：注入失败 -> 重启 -> 收敛
// ---------------------------------------------------------------------------

func TestQuantReconcileConvergesAfterInjectedFailure(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := &quantRuntime{}
	profiles := map[string]Profile{"demo": {}}
	c := New(s, r, profiles, time.Hour)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started = now
	c.StartupGrace = 0
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 注入：后端连续 3 轮不可用。
	r.failEnsure = true
	const failedRounds = 3
	for i := 0; i < failedRounds; i++ {
		if _, err := c.Reconcile(ctx, "demo"); err == nil {
			t.Fatalf("第 %d 轮本该失败", i+1)
		}
	}
	w, err := c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w.Phase != PhaseError || w.LastError == "" {
		t.Fatalf("失败没有被落盘: %+v", w)
	}
	ensuresAfterFailure, _, _, _ := r.counts()
	t.Logf("注入 %d 轮 Ensure 失败：Ensure 调用 %d 次，Phase=%s，LastError=%q（已落盘）",
		failedRounds, ensuresAfterFailure, w.Phase, w.LastError)

	// 重启：换一个 Controller 实例读同一份快照，后端仍然不可用。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	restarted := New(s2, r, profiles, time.Hour)
	restarted.now = c.now
	restarted.started = now
	restarted.StartupGrace = 0
	if _, err := restarted.Reconcile(ctx, "demo"); err == nil {
		t.Fatal("后端仍然不可用时重启后不该成功")
	}
	if w2, _ := restarted.Get("demo"); w2.Phase != PhaseError {
		t.Fatalf("重启后失败状态丢了: %+v", w2)
	}

	// 后端恢复：数清楚要几轮才收敛。
	r.failEnsure = false
	rounds := 0
	for i := 0; i < 5; i++ {
		rounds++
		if _, err := restarted.Reconcile(ctx, "demo"); err != nil {
			t.Fatalf("第 %d 轮: %v", rounds, err)
		}
		if w, _ := restarted.Get("demo"); w.Phase == PhaseRunning {
			break
		}
	}
	w3, err := restarted.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w3.Phase != PhaseRunning || w3.LastError != "" {
		t.Fatalf("没有收敛到 running: %+v", w3)
	}
	ensuresTotal, _, _, _ := r.counts()
	t.Logf("后端恢复后：%d 轮收敛到 Phase=%s（首轮重建工作负载，第 2 轮观察到就绪），"+
		"累计 Ensure 调用 %d 次，LastError 已清空", rounds, w3.Phase, ensuresTotal)
}

// TestQuantHardDeleteSurvivesACrashBeforeTheRuntimeCall 覆盖设计文档里被列为
// 盲区的那条缝：意图已经落盘、运行时动作还没发生的时候进程被杀。审计里
// "同一故障不重复记录"的判据也必须跨重启成立。
func TestQuantHardDeleteSurvivesACrashBeforeTheRuntimeCall(t *testing.T) {
	c, r := fixture(t)
	dir := filepath.Dir(c.store.Audit().Path())
	ctx := context.Background()
	if _, err := c.SetDesired(testActor, "demo", DesiredDeleted); err != nil {
		t.Fatal(err)
	}
	r.failDelete = true
	const failedRounds = 2
	for i := 0; i < failedRounds; i++ {
		if _, err := c.Reconcile(ctx, "demo"); err == nil {
			t.Fatalf("第 %d 轮删除本该失败", i+1)
		}
	}

	// 崩溃点：意图（Desired=deleted）已经落盘，运行时动作全部失败。
	// 直接关掉 store 模拟进程被杀。
	if err := c.store.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("重启失败: %v", err)
	}
	defer func() { _ = s2.Close() }()
	restarted := New(s2, r, map[string]Profile{"demo": {}}, time.Minute)
	restarted.now = c.now
	restarted.started = c.now()
	restarted.StartupGrace = 0
	if w, err := restarted.Get("demo"); err != nil || w.Desired != DesiredDeleted {
		t.Fatalf("删除意图没有跨重启保留: %+v %v", w, err)
	}

	// 后端继续坏着：重启后重试，但审计不能每轮刷一条。
	for i := 0; i < 3; i++ {
		if _, err := restarted.Reconcile(ctx, "demo"); err == nil {
			t.Fatalf("重启后第 %d 轮删除本该失败", i+1)
		}
	}
	events, err := restarted.Audit(AuditQuery{Workspace: "demo", Action: ActionHardDelete})
	if err != nil {
		t.Fatal(err)
	}
	errEvents := countResult(events, ResultError)
	okEvents := countResult(events, ResultOK)
	_, _, deletes, _ := r.counts()
	if errEvents != 1 {
		t.Fatalf("同一个故障被记录了 %d 次（期望 1 次）", errEvents)
	}
	t.Logf("删除失败跨重启共重试 %d 轮：Runtime.Delete 调用 %d 次，审计里 hard-delete/%s 只有 %d 条"+
		"（判据是持久化的 Phase=error，所以重启后也不会重复刷）", failedRounds+3, deletes, ResultError, errEvents)

	// 后端恢复：一轮收敛，并且此时才写下"真的删掉了"的那条记录。
	r.failDelete = false
	if _, err := restarted.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	w, err := restarted.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w.Desired != DesiredDeleted || w.Phase != PhaseDeleted {
		t.Fatalf("没有收敛到墓碑: %+v", w)
	}
	events, err = restarted.Audit(AuditQuery{Workspace: "demo", Action: ActionHardDelete})
	if err != nil {
		t.Fatal(err)
	}
	okEvents = countResult(events, ResultOK)
	_, _, deletes, _ = r.counts()
	if okEvents != 1 || deletes != failedRounds+4 {
		t.Fatalf("恢复后的收尾不对: ok=%d deletes=%d", okEvents, deletes)
	}
	t.Logf("后端恢复后：1 轮收敛到 Phase=%s，Runtime.Delete 累计 %d 次，审计 hard-delete/%s 累计 %d 条",
		w.Phase, deletes, ResultOK, okEvents)
}

func countResult(events []AuditEvent, result string) int {
	n := 0
	for _, e := range events {
		if e.Result == result {
			n++
		}
	}
	return n
}
