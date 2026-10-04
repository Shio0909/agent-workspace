package control

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// buildBackup 起一个控制器、制造多样状态、离线备份，返回
// （数据目录、备份文件、底层 fakeRuntime 的调用计数）。凭证能力由 credentials_test.go
// 里的 credRuntime 提供：Secret 的值永远不进控制面状态，这里也不存值。
func buildBackup(t *testing.T, dir string) (dataDir, archive string, r *fakeRuntime) {
	t.Helper()
	dataDir = filepath.Join(dir, "data")
	s, err := OpenStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	base := &credRuntime{}
	r = &base.fakeRuntime
	c := New(s, base, map[string]Profile{"demo": {CredentialPath: "/keys"}}, time.Minute)
	c.PollInterval = time.Millisecond

	// demo：running，凭证轮到版本 2（元数据进备份；Secret 本身不属于控制面）。
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetCredentials(testActor, "demo", map[string]string{"API_KEY": "v1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetCredentials(testActor, "demo", map[string]string{"API_KEY": "v2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	// idle：stopped 且带一个未过期的 lease。控制器语义只允许给 Running 的
	// 工作区发 lease，而"stopped + 活跃 lease"正是要固化的持久状态，所以这里
	// 直接通过 store 写入（与空闲回收保护的真实状态形态一致）。
	if _, err := c.Create(testActor, "idle", "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "idle", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "idle"); err != nil {
		t.Fatal(err)
	}
	idle, err := s.Get("idle")
	if err != nil {
		t.Fatal(err)
	}
	idle.Leases = map[string]time.Time{"tok": time.Now().Add(time.Hour)}
	if err := s.Put(idle); err != nil {
		t.Fatal(err)
	}
	// 两条幂等记录：一条已成功，一条留在 processing（模拟动作中途崩溃）。
	op, err := c.BeginOperation("biz-done", "demo", OpStart)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.FinishOperation(op, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginOperation("biz-stuck", "demo", OpRestart); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	archive = filepath.Join(dir, "backup.tar.gz")
	if err := Backup(dataDir, archive); err != nil {
		t.Fatal(err)
	}
	return dataDir, archive, r
}

// openSnapshot 打开一个数据目录，返回 id -> Workspace 的只读快照。
func openSnapshot(t *testing.T, dir string) map[string]Workspace {
	t.Helper()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	out := map[string]Workspace{}
	for _, w := range s.List() {
		out[w.ID] = w
	}
	return out
}

// queryAllAudit 查询一个数据目录的全部审计记录。
func queryAllAudit(t *testing.T, dir string) []AuditEvent {
	t.Helper()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	events, err := s.Audit().Query(AuditQuery{})
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func TestBackupRestoreRoundTripStateAndConvergence(t *testing.T) {
	dir := t.TempDir()
	dataDir, archive, _ := buildBackup(t, dir)

	// 数据相同：逐字段比较备份与恢复后的工作区记录。lease 是绝对过期时刻，
	// 断言逐值相等，而不是"剩余 TTL 相同"。
	before := openSnapshot(t, dataDir)
	restoredDir := filepath.Join(dir, "restored")
	if err := Restore(archive, restoredDir); err != nil {
		t.Fatal(err)
	}
	after := openSnapshot(t, restoredDir)
	if len(before) != len(after) || len(before) != 2 {
		t.Fatalf("workspace count changed: %d -> %d", len(before), len(after))
	}
	for id, want := range before {
		got, ok := after[id]
		if !ok {
			t.Fatalf("workspace %q lost", id)
		}
		if want.Desired != got.Desired || want.Phase != got.Phase ||
			want.CredentialVersion != got.CredentialVersion ||
			!want.LastActivity.Equal(got.LastActivity) || !want.UpdatedAt.Equal(got.UpdatedAt) {
			t.Fatalf("workspace %q drifted:\nbefore: %+v\nafter:  %+v", id, want, got)
		}
		for token, expires := range want.Leases {
			if !got.Leases[token].Equal(expires) {
				t.Fatalf("lease %q absolute expiry changed", token)
			}
		}
		if !want.CredentialUpdatedAt.Equal(got.CredentialUpdatedAt) {
			t.Fatalf("credential version %d timestamp drifted", want.CredentialVersion)
		}
	}

	// 审计可查且记录数一致。
	beforeAudit := queryAllAudit(t, dataDir)
	afterAudit := queryAllAudit(t, restoredDir)
	if len(beforeAudit) != len(afterAudit) || len(beforeAudit) == 0 {
		t.Fatalf("audit records changed: %d -> %d", len(beforeAudit), len(afterAudit))
	}

	// 行为正确：恢复后的控制器重新关联仍然存在的集群资源（fake runtime
	// 模拟"控制面丢了、集群还在"），且不产生多余的运行时动作。
	s, err := OpenStore(restoredDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := &credRuntime{fakeRuntime: fakeRuntime{running: true}}
	c := New(s, base, map[string]Profile{"demo": {CredentialPath: "/keys"}}, time.Minute)
	c.PollInterval = time.Millisecond
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if ensures, _, _, _ := base.counts(); ensures != 0 {
		t.Fatalf("restore caused a fresh Ensure on a converged workspace: %d", ensures)
	}
	w, err := c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w.Desired != DesiredRunning || w.Phase != PhaseRunning {
		t.Fatalf("restored workspace did not converge: %+v", w)
	}
}

func TestBackupRestoreOperationLifecycle(t *testing.T) {
	dir := t.TempDir()
	_, archive, _ := buildBackup(t, dir)
	restoredDir := filepath.Join(dir, "restored")
	if err := Restore(archive, restoredDir); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(restoredDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := &credRuntime{fakeRuntime: fakeRuntime{running: true}}
	c := New(s, r, map[string]Profile{"demo": {CredentialPath: "/keys"}}, time.Minute)

	// 已完成的操作：重放返回终态，不产生新副作用。
	ensuresBefore, _, _, _ := r.counts()
	if _, err := c.BeginOperation("biz-done", "demo", OpStart); !errors.Is(err, ErrOperationSucceeded) {
		t.Fatalf("completed op replay: %v", err)
	}
	if ensures, _, _, _ := r.counts(); ensures != ensuresBefore {
		t.Fatal("replayed completed op caused a side effect")
	}
	// processing 且租约未过期：按既定结论拒绝重复执行。
	if _, err := c.BeginOperation("biz-stuck", "demo", OpRestart); !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("unexpired processing op: %v", err)
	}
	// processing 且租约已过期：接管，代数前进。
	c.OperationLease = time.Nanosecond
	op, err := c.BeginOperation("biz-stuck", "demo", OpRestart)
	if err != nil {
		t.Fatalf("stale processing op not taken over: %v", err)
	}
	if op.Generation != 2 {
		t.Fatalf("takeover did not bump generation: %d", op.Generation)
	}
	if err := c.FinishOperation(op, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginOperation("biz-stuck", "demo", OpRestart); !errors.Is(err, ErrOperationSucceeded) {
		t.Fatalf("finished op after takeover: %v", err)
	}
}

func TestBackupRestoreExpiredLease(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	s, err := OpenStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	r := &fakeRuntime{}
	c := New(s, r, map[string]Profile{"demo": {}}, time.Minute)
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	// 直接写一条已过期的 lease：恢复耗时之后它必须表现得像没有 lease。
	w, err := s.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	w.Leases = map[string]time.Time{"tok": time.Now().Add(-time.Second)}
	if err := s.Put(w); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(dir, "b.tar.gz")
	if err := Backup(dataDir, archive); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // 确保恢复时刻晚于 lease 过期时刻
	restoredDir := filepath.Join(dir, "restored")
	if err := Restore(archive, restoredDir); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(restoredDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	c2 := New(s2, r, map[string]Profile{"demo": {}}, time.Minute)
	// 过期 lease 不能再阻止生命周期操作：hasLease 必须按绝对时刻判定。
	if _, err := c2.SetDesired(testActor, "demo", DesiredStopped); err != nil {
		t.Fatalf("expired lease still blocks lifecycle: %v", err)
	}
}

func TestBackupRefusesRunningController(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	s, err := OpenStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := Backup(dataDir, filepath.Join(dir, "b.tar.gz")); err == nil {
		t.Fatal("backed up a directory owned by a running controller")
	}
}

func TestRestoreRejectsCorruptArchive(t *testing.T) {
	dir := t.TempDir()
	dataDir, archive, _ := buildBackup(t, dir)
	_ = dataDir
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"truncated": raw[:len(raw)/2],
		// 破坏压缩载荷；gzip 尾部的 CRC 损坏另有回归测试覆盖。
		"corrupt": bytes.Replace(raw[:len(raw)/3], []byte{raw[len(raw)/3]}, []byte{raw[len(raw)/3] ^ 0xFF}, 1),
		"empty":   {},
	}
	for name, content := range cases {
		bad := filepath.Join(dir, "bad-"+name+".tar.gz")
		if err := os.WriteFile(bad, content, 0600); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(dir, "target-"+name)
		if err := Restore(bad, target); err == nil {
			t.Fatalf("%s: restored from a corrupt archive", name)
		}
		if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s: target directory left behind: %v", name, err)
		}
		// 暂存目录与目标同名：失败后必须被清理，不能留下半套数据。
		if leftovers, _ := filepath.Glob(target + ".restore-*"); len(leftovers) > 0 {
			t.Fatalf("%s: staging directory left behind: %v", name, leftovers)
		}
	}
}

func TestBackupInterruptedDoesNotPublishOrOverwrite(t *testing.T) {
	dir := t.TempDir()
	dataDir, archive, _ := buildBackup(t, dir)
	good, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}

	// 已有有效备份时拒绝再次备份到同一路径：重复运行不会覆盖。
	if err := Backup(dataDir, archive); err == nil {
		t.Fatal("second backup overwrote an existing archive")
	}
	after, err := os.ReadFile(archive)
	if err != nil || !bytes.Equal(good, after) {
		t.Fatalf("existing backup was disturbed: %v", err)
	}

	// 备份到不存在的目录：立刻失败，不在数据目录旁留下任何产物。
	if err := Backup(dataDir, filepath.Join(dir, "no-such-dir", "b.tar.gz")); err == nil {
		t.Fatal("backup into a missing directory succeeded")
	}
	if leftovers, _ := filepath.Glob(filepath.Join(dir, "no-such-dir", tmpPrefix+"*")); len(leftovers) > 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}

	// 模拟一次中断备份留下的临时文件：它是不完整的归档，恢复必须拒绝，
	// 也就是说临时文件不会被当成成功的产物。
	leftover := filepath.Join(dir, tmpPrefix+"archive-crash.tar.gz")
	if err := os.WriteFile(leftover, good[:len(good)/2], 0600); err != nil {
		t.Fatal(err)
	}
	if err := Restore(leftover, filepath.Join(dir, "from-crash")); err == nil {
		t.Fatal("restored from an interrupted backup's temp file")
	}
}

func TestRestoreRejectsUnsafeEntries(t *testing.T) {
	dir := t.TempDir()
	dataDir, archive, _ := buildBackup(t, dir)
	_ = dataDir

	// 从合法备份取出 state.db 字节，用来构造恶意变体。
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(zr)
	var stateBytes []byte
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == stateBackupName {
			stateBytes, _ = io.ReadAll(tr)
		}
	}
	if stateBytes == nil {
		t.Fatal("no state.db in fixture backup")
	}

	evil := []struct {
		label string
		name  string
		size  int64
		typ   byte
		body  bool
	}{
		{"path-traversal", "../escape", int64(len(stateBytes)), tar.TypeReg, true},
		{"absolute-path", "/etc/passwd", int64(len(stateBytes)), tar.TypeReg, true},
		{"symlink", "state.db", 0, tar.TypeSymlink, false},
		{"unknown-name", "extra.sh", 4, tar.TypeReg, true},
		{"oversized-entry", stateBackupName, maxRestoreFileBytes + 1, tar.TypeReg, false},
		{"decompression-bomb", stateBackupName, maxRestoreTotalBytes + 1, tar.TypeReg, false},
	}
	for _, tc := range evil {
		// 归档文件名只用 label（不含恶意路径），tar 成员名才携带恶意名字。
		path := filepath.Join(dir, "evil-"+tc.label+".tar.gz")
		if err := writeTarArchive(path, tc.name, tc.size, tc.typ, tc.body, stateBytes); err != nil {
			t.Fatal(err)
		}
		if err := Restore(path, filepath.Join(dir, "target-"+tc.label)); err == nil {
			t.Fatalf("%s: restore accepted a malicious archive", tc.label)
		}
	}
}

// writeTarArchive 打包一个单成员归档。body 为 false 时只写头部不写正文：
// 恢复路径必须在读取正文之前就按声明的头部大小拒绝（解压炸弹防护），
// 所以超大条目不真的写 2 GiB 字节。
func writeTarArchive(path, name string, size int64, typ byte, body bool, payload []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Size: size, Typeflag: typ, Mode: 0600}); err != nil {
		return err
	}
	if body {
		// 正文长度必须与头部声明一致；payload 只是内容来源。
		if _, err := tw.Write(payload[:min(size, int64(len(payload)))]); err != nil {
			return err
		}
	}
	// Close 的错误故意忽略：超大条目只写头部时 tar 流是故意残缺的，
	// 恢复路径必须在读正文前就按头部声明拒绝，轮不到 Close 善后。
	_ = tw.Close()
	_ = gz.Close()
	return nil
}

func TestRestoreRejectsManifestMismatch(t *testing.T) {
	dir := t.TempDir()
	dataDir, archive, _ := buildBackup(t, dir)
	_ = dataDir

	// 把 manifest 的计数改掉后重新打包：文件校验和全部完好，但 manifest
	// 与 state.db 的真实内容不一致，恢复必须拒绝。
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(zr)
	files := map[string][]byte{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(tr)
		files[h.Name] = b
	}
	var manifest BackupManifest
	if err := json.Unmarshal(files[manifestName], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.WorkspaceCount++
	forged, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	files[manifestName] = forged

	path := filepath.Join(dir, "forged.tar.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	for _, name := range []string{manifestName, stateBackupName, auditCurrentName} {
		b, ok := files[name]
		if !ok {
			continue
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(b)), Typeflag: tar.TypeReg, Mode: 0600}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	tw.Close()
	gz.Close()
	if err := Restore(path, filepath.Join(dir, "restored")); err == nil {
		t.Fatal("restored a backup whose manifest does not match its contents")
	}
}
