# Benchmarks

These are local `kind` comparisons used to find the next bottleneck, not
capacity claims. The repository ships the method and the scripts, not result
tables: numbers from a laptop-sized cluster do not transfer, and an earlier
headline figure turned out not to reproduce on a later commit (see below).
Run `scripts/benchmark-kind.sh` to produce your own. Raw output goes to
`benchmark-results/`, which is git-ignored.

## HTTP forwarding path

Setup: controller -> Kubernetes Service -> demo workspace, target `/health`,
keep-alive client, several rounds per variant, same kind cluster and cached demo
image for before/after comparisons.

What each change was meant to remove from the request path:

- **Ready-address cache.** The baseline probes readiness serially on every
  request. A short-lived cache of the ready endpoint removes that probe. It is
  invalidated on start/stop changes and on proxy connection or HTTP 503
  failures.
- **Activity off the fsync path.** `Acquire` and `release` used to persist
  activity to the local snapshot with a synchronous write and `fsync`. Counters
  are now kept in memory and flushed periodically.
- **Pooled upstream transport** for the proxy.
- **Informer-backed read cache** with event-driven reconciliation, so steady
  state does not issue a Deployment GET per request.

Findings that held up when re-checked on a later commit (two-node kind, kube
read cache and ready-address cache toggled independently, several rounds):

- The ready-address cache is the dominant factor. With it on, throughput is
  about the same with or without the informer.
- With the ready-address cache off, the informer helps a lot compared with a
  Deployment read per request, but stays far below the cached path. That
  configuration is deliberately degraded, so it is not a product number.
- Steady-state Deployment GETs drop to zero with the informer; the remaining
  discovery GET comes from the controller's readiness probe.

An earlier single-node comparison showed a much larger ratio for the cache. It
did not reproduce on a later commit and a two-node cluster, so no ratio is
quoted here.

Under load the controller was bound by request handling before CPU or memory.
The cold-start histograms (schedule, pull, ready, total) use Kubernetes
condition timestamps, which are usually second-granular, so phase sums are
coarse even though the total uses the controller's own clock.

## Idempotency

- Many concurrent requests with the same `biz_id` produce one side effect.
- With the control removed, the same requests produce one side effect each.
- Replaying an old successful request does not revive an already stopped
  workspace.

## Storage reclamation

- A suspended workspace across many reconciliation rounds: zero delete calls.
- Grace expiry: exactly one hard delete.
- Stop and restart paths never delete the PVC.

## Method notes

- Benchmarks exercise the storage and HTTP control path, not model inference.
- Failed experiments are kept in the raw output rather than deleted.
- Claims in this file are limited to what a rerun reproduced.
