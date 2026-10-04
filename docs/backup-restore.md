# Backup and restore (offline, control plane only)

## What this is — and is not

`agent-workspace -mode=backup` and `-mode=restore` copy and reinstate the
**control plane's local state**: the bbolt state database (workspaces,
idempotency records, schema) and the audit log files. This is an **offline**
flow by construction: backup takes the source data directory's lock; restore
only publishes to a nonexistent destination and carries a held controller lock
with the staged directory. Existing or concurrently created targets are never
removed or replaced. There is no online backup in this version;
one would need a trigger inside the running controller process and is deferred
until needed.

Two things the backup deliberately does not contain:

- **Kubernetes objects.** Deployments, Services, PVCs and Secrets live in etcd.
  The backup never copies them, and restore never touches them. The recovery
  story is *converge, not copy*: the restored controller re-associates with
  whatever cluster resources still exist and reconciles them toward the
  recorded intent. A restore drill therefore verifies re-association, not a
  whole-cluster disaster recovery.
- **Secret and PVC data.** A restore to a fresh cluster would leave workspaces
  whose storage and credentials are gone; that scenario is out of scope for
  this tool and belongs to cluster-level backup.

## Consistency semantics (honest version)

- `state.db` is exported through a bbolt read-only transaction
  (`tx.WriteTo`), which yields a page-consistent snapshot. The manifest's
  workspace/operation counts are read back from **those snapshot bytes**, not
  from a memory list.
- The state database and the audit log have **no common transaction**: the
  controller persists business state first and appends audit afterwards. The
  offline precondition removes concurrent writers, but the audit log remains a
  supplementary record, not the recovery basis: an audit append can fail while
  the business operation it describes succeeds, and that gap is visible only
  through `nc_audit_failures_total`. Restore treats `state.db` as the source
  of truth.
- `FlushActivity` is a controller behavior that persists in-memory request
  activity on graceful shutdown. It runs during normal stop, but a crash (for
  example `kill -9`) can still lose up to one flush interval of activity
  timestamps. That loss is safe for reclamation (restart grace covers it) and
  it is not what defines the backup RPO.
- **RPO is the time since the last successful backup**, not the activity
  flush interval. Scheduling backups is an operator concern; the tool only
  guarantees that a published backup is complete and verifiable.

## Archive format

A gzip-compressed tar with a fixed member list:

| Member | Purpose |
| --- | --- |
| `manifest.json` | format version, schema, counts, per-file size and SHA-256 |
| `state.db` | page-consistent bbolt snapshot (the recovery basis) |
| `audit.jsonl` | current audit file, if present |
| `audit.jsonl.<20-digit sequence>` | rotated audit archives, if present |

`controller.lock` is not archived. The schema marker inside `state.db` must
match both the manifest declaration and the version this binary interprets.
Manifest file names use the same fixed allowlist as tar members, and every
extracted file must have exactly one manifest size and SHA-256 entry. Gzip
CRC/trailer validation runs to EOF under a decompressed-stream byte budget.

## What backup does

1. Locks the data directory (`controller.lock`, non-blocking). A running
   controller makes this fail: offline only.
2. Snapshots `state.db` via a read-only bbolt transaction.
3. Copies the audit files, computing SHA-256 and size for each member.
4. Counts workspaces and operations **from the snapshot itself**.
5. Writes the archive to a temp file, fsyncs, then **verifies by restoring it
   into a scratch directory** using the same code path a real restore takes.
6. Publishes using an atomic no-replace rename (Linux/macOS). An existing
   file, directory, or symlink at the destination is never overwritten. An
   interruption before publication can leave temporary files (names start
   with `.tmp-`), which are not successful backup outputs. The final parent
   directory fsync follows publication; its failure is handled as described
   below.

## What restore does

1. Requires a nonexistent target path, including rejecting empty directories
   and dangling symlinks. Choose a new path; restore never deletes a target.
2. Unpacks into a staging directory (`<target>.restore-<random>`), accepting
   only the member names above. Absolute paths, path traversal, symlinks,
   hardlinks, unknown names, duplicate names and entries declaring more than
   the per-file / total byte caps are rejected before their contents are read.
3. Verifies the complete gzip stream, the manifest/member correspondence,
   checksums, schema, and counts. Restored files are individually fsynced.
4. Holds the staged controller lock, fsyncs the staging directory, and publishes
   with an atomic no-replace rename. The lock moves with the directory and
   prevents a controller opening it until publication completes. Finally fsyncs
   the actual destination parent directory. A concurrently created target
   causes failure and is preserved.

Before publication, failure cleans the staging directory. If the final parent
fsync fails after publication, the command returns an error but leaves the
complete published result in place; inspect it rather than blindly retrying.
An interruption likewise may leave an unpublished temporary file/directory.
Power-loss recovery has not been tested. No-replace rename is supported on
Linux and macOS; unsupported platforms or filesystems fail without falling
back to an overwrite operation.

## Drill

`go test ./internal/control -run 'TestBackup|TestRestore|TestPublication|TestPublished'`
covers the in-process path. A kind-level drill (not executed here) looks like:

```bash
# 1. run a controller, create state (workspaces, credential versions, sessions on PVC)
# 2. stop the controller (graceful: FlushActivity runs)
agent-workspace -mode=backup -data data -out backup.tar.gz

# 3. remove or move the old data directory; restore into a fresh one
agent-workspace -mode=restore -data data-restored -in backup.tar.gz

# 4. start a controller on data-restored and verify:
#    - GET /v1/workspaces matches the pre-backup list (workspaces, phases,
#      credential versions — metadata only, never values)
#    - replaying a completed biz_id is a no-op; a stale processing record is
#      taken over after the lease expires
#    - kubectl shows the same Deployments/PVCs, still owned (labels), and the
#      controller re-adopts them without recreating anything
#    - data written through the gateway before the backup still reads back
```

The restore is control-plane only: Secret and PVC contents were never copied
and must still exist in the cluster for the drill to make sense.

## Test coverage

`internal/control/backup_test.go` covers: round-trip equality (including
absolute lease expiry), convergence after restore (no spurious `Ensure`),
completed-operation replay without side effects, processing-operation
takeover, expired-lease behavior, refusal to run against a live controller,
corrupt/truncated/empty archives, refusal to overwrite, interrupted-backup
temp files being rejected, unsafe archive members (traversal, absolute path,
symlink, unknown names, oversized and bomb entries), and manifest/contents
mismatch.

Regression tests also cover a destination acquired by a controller during
extraction, an output created during backup, manifest path traversal and missing
checksums, schema declaration mismatch, gzip CRC failure, refusal of existing
empty directories and symlinks, and the controller lock surviving publication.
