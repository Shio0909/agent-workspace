package control

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func rewriteBackupManifest(t *testing.T, src, dst string, change func(*BackupManifest)) {
	t.Helper()
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	var out bytes.Buffer
	zw := gzip.NewWriter(&out)
	tw := tar.NewWriter(zw)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == manifestName {
			var m BackupManifest
			if err := json.Unmarshal(body, &m); err != nil {
				t.Fatal(err)
			}
			change(&m)
			body, err = json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			h.Size = int64(len(body))
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreRejectsAuditNotCoveredByManifest(t *testing.T) {
	dir := t.TempDir()
	_, archive, _ := buildBackup(t, dir)
	bad := filepath.Join(dir, "unverified-audit.tar.gz")
	rewriteBackupManifest(t, archive, bad, func(m *BackupManifest) { delete(m.Files, auditCurrentName) })
	if err := Restore(bad, filepath.Join(dir, "restored")); err == nil {
		t.Fatal("restore accepted an audit file excluded from manifest checksums")
	}
}

func TestRestoreRejectsManifestTraversal(t *testing.T) {
	dir := t.TempDir()
	_, archive, _ := buildBackup(t, dir)
	payload := []byte("external file, not a backup member")
	if err := os.WriteFile(filepath.Join(dir, "outside"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	bad := filepath.Join(dir, "manifest-traversal.tar.gz")
	rewriteBackupManifest(t, archive, bad, func(m *BackupManifest) {
		m.Files["../outside"] = BackupFile{Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}
	})
	if err := Restore(bad, filepath.Join(dir, "restored")); err == nil {
		t.Fatal("restore verified a manifest path outside its staging directory")
	}
}

func TestRestoreRejectsManifestSchemaMismatch(t *testing.T) {
	dir := t.TempDir()
	_, archive, _ := buildBackup(t, dir)
	bad := filepath.Join(dir, "wrong-schema.tar.gz")
	rewriteBackupManifest(t, archive, bad, func(m *BackupManifest) { m.Schema = "unsupported" })
	if err := Restore(bad, filepath.Join(dir, "restored")); err == nil {
		t.Fatal("restore ignored manifest schema")
	}
}

func TestRestoreRejectsBadGzipTrailer(t *testing.T) {
	dir := t.TempDir()
	_, archive, _ := buildBackup(t, dir)
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-8] ^= 0xff
	bad := filepath.Join(dir, "bad-trailer.tar.gz")
	if err := os.WriteFile(bad, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := Restore(bad, filepath.Join(dir, "restored")); err == nil {
		t.Fatal("restore accepted gzip checksum corruption")
	}
}

func TestRestoreDoesNotRemoveTargetCreatedDuringExtraction(t *testing.T) {
	dir := t.TempDir()
	_, archive, _ := buildBackup(t, dir)
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	pipe := filepath.Join(dir, "blocked-input")
	if err := syscall.Mkfifo(pipe, 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target")
	done := make(chan error, 1)
	go func() { done <- Restore(pipe, target) }()
	// Restore 已检查目标不存在，创建暂存目录后阻塞在打开归档。
	deadline := time.Now().Add(2 * time.Second)
	for {
		paths, _ := filepath.Glob(target + ".restore-*")
		if len(paths) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restore did not create staging directory")
		}
		time.Sleep(time.Millisecond)
	}
	store, err := OpenStore(target)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sentinel := filepath.Join(target, "must-survive")
	if err := os.WriteFile(sentinel, []byte("live controller data"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(pipe, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(raw); err != nil {
		t.Fatal(err)
	}
	f.Close()
	restoreErr := <-done
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("restore removed a live controller directory: restore=%v, sentinel=%v", restoreErr, err)
	}
	if restoreErr == nil {
		t.Fatal("restore accepted a target acquired by another controller")
	}
}

func TestBackupDoesNotOverwriteOutputCreatedDuringStaging(t *testing.T) {
	dir := t.TempDir()
	data, _, _ := buildBackup(t, dir)
	audit := filepath.Join(data, auditCurrentName)
	if err := os.Remove(audit); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(audit, 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "new-backup.tar.gz")
	done := make(chan error, 1)
	go func() { done <- Backup(data, output) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		paths, _ := filepath.Glob(filepath.Join(dir, tmpPrefix+"stage-*"))
		if len(paths) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("backup did not stage state")
		}
		time.Sleep(time.Millisecond)
	}
	sentinel := []byte("another process owns this output")
	if err := os.WriteFile(output, sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(audit, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	backupErr := <-done
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Fatalf("backup overwrote a newly-created output: backup=%v", backupErr)
	}
	if backupErr == nil {
		t.Fatal("backup published over an existing output")
	}
}

func TestRestoreRejectsExistingEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	_, archive, _ := buildBackup(t, dir)
	target := filepath.Join(dir, "empty-target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Restore(archive, target); err == nil {
		t.Fatal("restore replaced an existing empty directory")
	}
	entries, err := os.ReadDir(target)
	if err != nil || len(entries) != 0 {
		t.Fatalf("target changed: %v, %v", entries, err)
	}
}

func TestPublicationRefusesExistingDestinations(t *testing.T) {
	for _, kind := range []string{"file", "empty-directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			from, to := filepath.Join(dir, "source"), filepath.Join(dir, "target")
			if err := os.WriteFile(from, []byte("new"), 0600); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "file":
				if err := os.WriteFile(to, []byte("old"), 0600); err != nil {
					t.Fatal(err)
				}
			case "empty-directory":
				if err := os.Mkdir(to, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink("missing", to); err != nil {
					t.Fatal(err)
				}
			}
			if err := publishNoReplace(from, to); !errors.Is(err, os.ErrExist) {
				t.Fatalf("publication must refuse existing %s: %v", kind, err)
			}
			if data, err := os.ReadFile(from); err != nil || string(data) != "new" {
				t.Fatalf("source changed: %q, %v", data, err)
			}
			if kind == "file" {
				if data, err := os.ReadFile(to); err != nil || string(data) != "old" {
					t.Fatalf("destination changed: %q, %v", data, err)
				}
			} else if _, err := os.Lstat(to); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublishedDirectoryKeepsControllerLock(t *testing.T) {
	dir := t.TempDir()
	staging, target := filepath.Join(dir, "staging"), filepath.Join(dir, "target")
	if err := os.Mkdir(staging, 0700); err != nil {
		t.Fatal(err)
	}
	release, err := lockDataDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := publishNoReplace(staging, target); err != nil {
		t.Fatal(err)
	}
	if store, err := OpenStore(target); err == nil {
		store.Close()
		t.Fatal("controller acquired a directory still held by restore")
	}
}
