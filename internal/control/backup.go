package control

// 离线备份与恢复。
//
// 边界（第一版明确不做的事，见 docs/backup-restore.md）：
//   - 只做离线备份。Backup 自己会尝试独占数据目录：控制器还在运行时 flock
//     拿不到，备份直接拒绝。在线备份需要由持有 Store 的控制器进程提供触发
//     入口，另行设计，不要把两条路线混在一个入口里。
//   - 备份的是控制面状态：bbolt 快照 + 审计文件。Secret、PVC、Deployment 的
//     真实来源是 Kubernetes（etcd），不在备份里。恢复演练验证的是"控制面
//     恢复后重新关联仍然存在的集群资源"，不是整个集群的灾难恢复。
//   - state.db 快照和审计文件之间没有共同事务：业务状态先落盘、审计后追加。
//     离线备份消除了并发窗口，但审计仍是补充记录——审计追加失败时业务操作
//     本来就可能已经成功，恢复以 state.db 为准。
//
// 一致性与原子性：
//   - state.db 用 bbolt 只读事务的 tx.WriteTo 导出，是页面级一致快照。
//     manifest 的计数从同一份快照字节统计，不读内存列表。
//   - 备份先写临时文件、关闭压缩流、fsync、重新解包验证校验和与 schema，
//     最后原子改名发布。中断不会产生"看似完整的备份"，也不会覆盖已有文件。
//   - 恢复先解包到暂存目录，验证通过后整体改名发布；失败清理暂存，目标
//     目录不会留下半套数据。

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	// backupFormatVersion 是备份归档格式的版本。恢复只接受自己认识的版本；
	// 未来改格式时升这个数字并保留旧版本的读取路径（或提供迁移）。
	backupFormatVersion = 1
	// 解包防护：防止解压炸弹和意外大文件。上限只约束恢复路径，不影响备份。
	maxRestoreFileBytes  = 2 << 30 // 单个条目 2 GiB
	maxRestoreTotalBytes = 4 << 30 // 全部条目合计 4 GiB
	maxManifestBytes     = 1 << 20
	// tar 头部与尾部也有解压预算，不能在 tar EOF 后无限读取 gzip。
	maxRestoreArchiveBytes = maxRestoreTotalBytes + (16 << 20)
	backupLockTimeout      = time.Second
	tmpPrefix              = ".tmp-"
)

// BackupFile 是归档里一个成员的校验记录。
type BackupFile struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// BackupManifest 描述一份备份。WorkspaceCount/OperationCount 从归档内的
// state.db 快照统计，与归档字节一一对应。
type BackupManifest struct {
	FormatVersion  int                   `json:"format_version"`
	CreatedAt      time.Time             `json:"created_at"`
	Schema         string                `json:"schema"`
	WorkspaceCount int                   `json:"workspace_count"`
	OperationCount int                   `json:"operation_count"`
	Files          map[string]BackupFile `json:"files"`
}

// stateFileBackupName 等成员名是固定的，恢复时按名单收，拒绝其余一切名字。
const (
	manifestName        = "manifest.json"
	stateBackupName     = "state.db"
	auditCurrentName    = "audit.jsonl"
	auditArchivePattern = ".%020d"
)

// Backup 把数据目录导出成一个 tar.gz。
//
// 前提是离线：控制器持有 controller.lock 时拒绝执行。先快照 state.db（只读
// 事务），再复制审计文件，校验和与计数都从同一份字节统计，最后经"临时文件 →
// fsync → 重新解包验证 → 原子改名"发布。outPath 已存在时拒绝覆盖。
func Backup(dataDir, outPath string) error {
	dataDir = filepath.Clean(dataDir)
	outPath = filepath.Clean(outPath)
	if _, err := os.Stat(dataDir); err != nil {
		return fmt.Errorf("data directory: %w", err)
	}
	if _, err := os.Lstat(outPath); err == nil {
		return fmt.Errorf("%s already exists; refusing to overwrite an existing backup", outPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// 独占数据目录：控制器在运行时这里必然失败，离线语义由此强制。
	release, err := lockDataDir(dataDir)
	if err != nil {
		return err
	}
	defer release()

	// state.db 快照：只读事务导出页面。控制器已停止，此刻没有并发写。
	snapshotPath, err := snapshotStateDB(filepath.Join(dataDir, stateFile), outPath)
	if err != nil {
		return err
	}
	defer os.Remove(snapshotPath)

	schema, workspaces, operations, err := summarizeState(snapshotPath)
	if err != nil {
		return fmt.Errorf("read state snapshot: %w", err)
	}
	if schema != schemaValue {
		return fmt.Errorf("unknown schema %q; refusing to back up a database this version cannot interpret", schema)
	}

	files := map[string]BackupFile{}
	staged := map[string]string{} // 归档成员名 -> 暂存文件
	defer func() {
		for _, path := range staged {
			os.Remove(path)
		}
	}()
	outDir := filepath.Dir(outPath)
	// 成员名显式给定，不能从文件路径推导：快照是随机临时名，必须映射到
	// 归档里的规范名字 state.db。
	if staged[stateBackupName], err = stageFile(outDir, stateBackupName, snapshotPath, files); err != nil {
		return err
	}
	auditPath := filepath.Join(dataDir, auditCurrentName)
	if _, err := os.Stat(auditPath); err == nil {
		if staged[auditCurrentName], err = stageFile(outDir, auditCurrentName, auditPath, files); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	archives, err := auditArchivePaths(auditPath)
	if err != nil {
		return err
	}
	for _, path := range archives {
		name := filepath.Base(path)
		if staged[name], err = stageFile(outDir, name, path, files); err != nil {
			return err
		}
	}

	manifest := BackupManifest{
		FormatVersion:  backupFormatVersion,
		CreatedAt:      time.Now().UTC(),
		Schema:         schema,
		WorkspaceCount: workspaces,
		OperationCount: operations,
		Files:          files,
	}
	return publishArchive(outPath, manifest, staged)
}

// lockDataDir 对 controller.lock 做 flock，返回释放函数。控制器运行时这里
// 一定失败，备份和恢复都借此保证离线。
func lockDataDir(dataDir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dataDir, "controller.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data directory is in use; offline backup requires the controller to be stopped: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// snapshotStateDB 把 bbolt 数据库用只读事务导出为独立文件。ReadOnly 打开在
// 控制器持锁时会失败（Timeout 生效），是离线前提的又一道防线。
func snapshotStateDB(statePath, outPath string) (string, error) {
	db, err := bolt.Open(statePath, 0600, &bolt.Options{ReadOnly: true, Timeout: backupLockTimeout})
	if err != nil {
		return "", fmt.Errorf("open state database (is the controller running?): %w", err)
	}
	defer db.Close()
	tmp, err := os.CreateTemp(filepath.Dir(outPath), tmpPrefix+"state-*.db")
	if err != nil {
		return "", err
	}
	defer tmp.Close()
	err = db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(tmp)
		return err
	})
	if err != nil {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("snapshot state database: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// summarizeState 从快照文件本身读取 schema 与记录数。manifest 描述的就是
// 这份字节，而不是打开中的内存列表。
func summarizeState(snapshotPath string) (schema string, workspaces, operations int, err error) {
	db, err := bolt.Open(snapshotPath, 0600, &bolt.Options{ReadOnly: true, Timeout: backupLockTimeout})
	if err != nil {
		return "", 0, 0, err
	}
	defer db.Close()
	err = db.View(func(tx *bolt.Tx) error {
		if meta := tx.Bucket(metaBucket); meta != nil {
			schema = string(meta.Get(schemaKey))
		}
		count := func(b *bolt.Bucket) int {
			n := 0
			if b == nil {
				return 0
			}
			_ = b.ForEach(func([]byte, []byte) error { n++; return nil })
			return n
		}
		workspaces = count(tx.Bucket(workspacesBucket))
		operations = count(tx.Bucket(operationsBucket))
		return nil
	})
	return schema, workspaces, operations, err
}

// stageFile 复制一个源文件到 stageDir 下的临时文件，并把以 memberName 为键
// 的校验和记进 files。成员名与源文件名解耦：快照的临时文件名不能泄漏进归档。
func stageFile(stageDir, memberName, path string, files map[string]BackupFile) (string, error) {
	src, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer src.Close()
	tmp, err := os.CreateTemp(stageDir, tmpPrefix+"stage-*")
	if err != nil {
		return "", err
	}
	defer tmp.Close()
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, h), src)
	if err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	files[memberName] = BackupFile{Size: size, SHA256: hex.EncodeToString(h.Sum(nil))}
	return tmp.Name(), nil
}

// publishArchive 把暂存文件打包成 tar.gz：临时文件写入 → fsync → 解包验证 →
// 原子改名。验证走的是恢复路径同一套检查，所以"能通过验证"和"能被恢复"是
// 同一个标准。
func publishArchive(outPath string, manifest BackupManifest, staged map[string]string) error {
	tmp, err := os.CreateTemp(filepath.Dir(outPath), tmpPrefix+"archive-*.tar.gz")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer tmp.Close()
	defer os.Remove(tmpName)

	gz := gzip.NewWriter(tmp)
	tw := tar.NewWriter(gz)
	writeEntry := func(name string, path string, fi BackupFile) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0600, Size: fi.Size, Typeflag: tar.TypeReg, ModTime: time.Now(),
		}); err != nil {
			return err
		}
		if _, err := io.Copy(tw, f); err != nil {
			return fmt.Errorf("write entry %q: %w", name, err)
		}
		return nil
	}
	// manifest 最后写：它描述的校验和此时才齐全。
	for _, name := range sortedStagedNames(staged) {
		if err := writeEntry(name, staged[name], manifest.Files[name]); err != nil {
			return err
		}
	}
	mb, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: manifestName, Mode: 0600, Size: int64(len(mb)), Typeflag: tar.TypeReg, ModTime: time.Now(),
	}); err != nil {
		return err
	}
	if _, err := tw.Write(mb); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// 发布前的独立验证：重新解包，校验和、schema、计数必须全部对上。
	verifyDir := tmpName + ".verify"
	verifiedManifest, err := extractArchive(tmpName, verifyDir)
	if err != nil {
		os.RemoveAll(verifyDir)
		return fmt.Errorf("backup verification failed: %w", err)
	}
	if err := verifyRestoredState(verifyDir, *verifiedManifest); err != nil {
		os.RemoveAll(verifyDir)
		return fmt.Errorf("backup verification failed: %w", err)
	}
	os.RemoveAll(verifyDir)

	if err := publishNoReplace(tmpName, outPath); err != nil {
		return err
	}
	return syncParentDir(outPath)
}

func sortedStagedNames(staged map[string]string) []string {
	names := make([]string, 0, len(staged))
	for name := range staged {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Restore 只恢复到不存在的新目录，不删除任何目标路径。暂存目录验证完成后
// 持有其中的 controller.lock，并以不覆盖方式发布；控制器在发布期间无法打开
// 新目录，目标在解包期间被其他进程创建也只会令发布失败。
func Restore(backupPath, dataDir string) error {
	backupPath = filepath.Clean(backupPath)
	dataDir = filepath.Clean(dataDir)
	if _, err := os.Lstat(dataDir); err == nil {
		return fmt.Errorf("target directory %s already exists; restore requires a new directory", dataDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(dataDir), filepath.Base(dataDir)+".restore-*")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			os.RemoveAll(staging)
		}
	}()
	manifest, err := extractArchive(backupPath, staging)
	if err != nil {
		return err
	}
	if err := verifyRestoredState(staging, *manifest); err != nil {
		return err
	}
	// 锁文件与目录一起移动，避免锁住旧 inode 却发布一个无锁的新目录。
	release, err := lockDataDir(staging)
	if err != nil {
		return err
	}
	defer release()
	if err := syncDir(staging); err != nil {
		return err
	}
	if err := publishNoReplace(staging, dataDir); err != nil {
		return err
	}
	published = true
	return syncParentDir(dataDir)
}

// extractArchive 解包一份备份到目录。只接受固定名单里的成员名（路径穿越、
// 绝对路径、符号链接全部拒绝），单文件与总量都有上限，重复名字拒绝。
func extractArchive(archivePath, destDir string) (*BackupManifest, error) {
	if err := os.MkdirAll(destDir, 0700); err != nil {
		return nil, err
	}
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("not a valid gzip archive: %w", err)
	}
	defer gz.Close()
	decoded := &io.LimitedReader{R: gz, N: maxRestoreArchiveBytes + 1}
	tr := tar.NewReader(decoded)

	var manifest *BackupManifest
	seen := map[string]bool{}
	var total int64
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("archive is truncated or corrupt: %w", err)
		}
		name := header.Name
		if !validArchiveName(name) {
			return nil, fmt.Errorf("unexpected archive entry %q", name)
		}
		if header.Typeflag != tar.TypeReg {
			return nil, fmt.Errorf("archive entry %q is not a regular file", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate archive entry %q", name)
		}
		seen[name] = true
		limit := int64(maxRestoreFileBytes)
		if name == manifestName {
			limit = maxManifestBytes
		}
		if header.Size > limit {
			return nil, fmt.Errorf("archive entry %q exceeds %d bytes", name, limit)
		}
		total += header.Size
		if total > maxRestoreTotalBytes {
			return nil, fmt.Errorf("archive contents exceed %d bytes", maxRestoreTotalBytes)
		}
		dest := filepath.Join(destDir, name)
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			return nil, err
		}
		written, err := io.Copy(out, tr)
		if err == nil {
			err = out.Sync()
		}
		closeErr := out.Close()
		if err != nil {
			return nil, fmt.Errorf("unpack %q: %w", name, err)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if written != header.Size {
			return nil, fmt.Errorf("archive entry %q is truncated", name)
		}
		if name == manifestName {
			manifest, err = parseManifestFile(dest)
			if err != nil {
				return nil, err
			}
		}
	}
	// tar 的 EOF 不保证 gzip 尾部已经被读取。读完受限流，触发 CRC/ISIZE
	// 验证；gzip.Close 本身不验证校验和。
	if _, err := io.Copy(io.Discard, decoded); err != nil {
		return nil, fmt.Errorf("gzip archive is truncated or corrupt: %w", err)
	}
	if decoded.N == 0 {
		return nil, fmt.Errorf("decompressed archive exceeds %d bytes", maxRestoreArchiveBytes)
	}
	if manifest == nil {
		return nil, errors.New("archive has no manifest.json")
	}
	if len(seen) != len(manifest.Files)+1 {
		return nil, errors.New("archive members do not match manifest files")
	}
	for name := range seen {
		if name != manifestName {
			if _, ok := manifest.Files[name]; !ok {
				return nil, fmt.Errorf("archive member %q has no manifest checksum", name)
			}
		}
	}
	return manifest, nil
}

// validArchiveName 只放行名单内的成员名。这意味着路径穿越和绝对路径天然
// 被拒绝；不解包任何未知名字的条目。
func validArchiveName(name string) bool {
	if name == manifestName || name == stateBackupName || name == auditCurrentName {
		return true
	}
	if suffix, ok := strings.CutPrefix(name, auditCurrentName+"."); ok {
		if len(suffix) != 20 || strings.Trim(suffix, "0123456789") != "" {
			return false
		}
		_, err := strconv.ParseUint(suffix, 10, 64)
		return err == nil
	}
	return false
}

func parseManifestFile(path string) (*BackupManifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var m BackupManifest
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("manifest must contain one JSON object")
	}
	if m.FormatVersion != backupFormatVersion {
		return nil, fmt.Errorf("backup format version %d is not supported (want %d)", m.FormatVersion, backupFormatVersion)
	}
	if len(m.Files) == 0 || m.Files[stateBackupName].SHA256 == "" {
		return nil, errors.New("manifest lists no state.db")
	}
	if m.Schema != schemaValue || m.WorkspaceCount < 0 || m.OperationCount < 0 {
		return nil, errors.New("manifest schema or counts are invalid")
	}
	for name, file := range m.Files {
		if !validArchiveName(name) || name == manifestName {
			return nil, fmt.Errorf("unexpected manifest file %q", name)
		}
		digest, err := hex.DecodeString(file.SHA256)
		if file.Size < 0 || file.Size > maxRestoreFileBytes || err != nil || len(digest) != sha256.Size {
			return nil, fmt.Errorf("invalid manifest size or checksum for %q", name)
		}
	}
	return &m, nil
}

// verifyRestoredState 是备份与恢复共用的发布前检查：文件校验和、schema、
// 计数。任何一步不过，恢复都不发布。
func verifyRestoredState(dir string, manifest BackupManifest) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) != len(manifest.Files)+1 {
		return errors.New("restored files do not match manifest")
	}
	for _, entry := range entries {
		if entry.Name() == manifestName {
			continue
		}
		if _, ok := manifest.Files[entry.Name()]; !ok || !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected restored file %q", entry.Name())
		}
	}
	for name, want := range manifest.Files {
		if !validArchiveName(name) || name == manifestName {
			return fmt.Errorf("unexpected manifest file %q", name)
		}
		h := sha256.New()
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("manifest lists %q but it is missing", name)
		}
		size, err := io.Copy(h, f)
		f.Close()
		if err != nil {
			return err
		}
		if size != want.Size || hex.EncodeToString(h.Sum(nil)) != want.SHA256 {
			return fmt.Errorf("checksum mismatch for %q", name)
		}
	}
	schema, workspaces, operations, err := summarizeState(filepath.Join(dir, stateBackupName))
	if err != nil {
		return fmt.Errorf("restored state database cannot be opened: %w", err)
	}
	if schema != schemaValue || schema != manifest.Schema {
		return fmt.Errorf("restored database has schema %q; want %q", schema, schemaValue)
	}
	if workspaces != manifest.WorkspaceCount || operations != manifest.OperationCount {
		return fmt.Errorf("restored database counts do not match manifest: workspaces=%d/%d operations=%d/%d",
			workspaces, manifest.WorkspaceCount, operations, manifest.OperationCount)
	}
	return nil
}

// auditArchivePaths 列出与 base 同目录的 audit.jsonl.<20 位序号> 归档。
func auditArchivePaths(base string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Dir(base))
	if err != nil {
		return nil, err
	}
	prefix := filepath.Base(base) + "."
	var paths []string
	for _, entry := range entries {
		suffix, ok := strings.CutPrefix(entry.Name(), prefix)
		if entry.IsDir() || !ok || len(suffix) != 20 || strings.Trim(suffix, "0123456789") != "" {
			continue
		}
		if _, err := strconv.ParseUint(suffix, 10, 64); err != nil {
			continue
		}
		paths = append(paths, filepath.Join(filepath.Dir(base), entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

func syncParentDir(path string) error {
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
