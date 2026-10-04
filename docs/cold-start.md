# Cold start: what the wait is made of, and what a user can see

## The three phases the controller can measure today

Cold-start phase metrics come from **pod condition and container status
timestamps** read through the informer cache (`StartupTimestamps` in
`internal/kube/watch.go`), exported as the
`nc_workspace_start_seconds{phase}` histogram with four label values:

| Phase | Interval | What it actually measures |
| --- | --- | --- |
| `schedule` | t0 → `PodScheduled` | controller wake-up + API round-trip + scheduler decision |
| `pull` | `PodScheduled` → container `StartedAt` | **image pull and container start, merged** — pod conditions do not expose kubelet's pull events |
| `ready` | container `StartedAt` → `PodReady` | application boot + readiness probe |
| `total` | t0 → first ready reconcile | measured on the controller's real clock |

Honesty notes that matter when reading reports:

- The `pull` label is a **merged segment**. Splitting pull from container
  start needs the kube `Event` resources (`Pulling`/`Pulled`), which are a
  separate resource type, best-effort, and not always present or pairable —
  treat them as supplementary evidence, never as the primary metric.
- Phase sums are **not** `total`: `total` starts at the controller's t0 with
  nanosecond resolution while phases start from second-granular condition
  timestamps (the base is truncated to a second, see
  `observeStartPhases`). Compare distributions, not sums.
- A phase with a missing endpoint timestamp is skipped, not imputed.

## What a user sees while waiting

While a workspace cold-starts, a caller is inside `Acquire` (the forwarded
request or the reconcile that was woken). Today the observable state is:

- `GET /v1/workspaces/{id}` reports `phase: starting` (durable) plus the
  controller-side signals: `last_error` on failures, and the metrics above
  once a start completes.
- There is deliberately **no phase streaming endpoint yet**. The internal
  wake-up channel (`slot.wake`) is a capacity-1 notification slot with
  draining, not a broadcast bus; wiring SSE to it would silently drop phase
  transitions for all but one subscriber. If streaming becomes a requirement,
  design a proper broadcast first (per-subscriber channels, unsubscribe,
  slow-client policy) — see the note in `internal/control/controller.go`.

For the first version, "what the user can see" is: `starting` vs `running`,
how long such waits historically take (the histograms), and the audit trail
of lifecycle events.

## A cache-controlled measurement protocol

A "cold pull" claim requires the node's image cache to actually miss. Swapping
to a new tag is **not** sufficient: unchanged layers hit the cache, and a
re-pulled manifest can still reuse layers. What works on a kind cluster:

1. **Pin content**: build a distinct image digest per run (`docker build --pull`
   with an incremented layer, e.g. an extra 1 MiB random file), or reference by
   digest so content-addressing works for you.
2. **Control the node**: the workspace runs on one worker node; evict the image
   from that node's containerd before each run:
   `docker exec <kind-worker> crictl rmi <image-digest>`. Verify with
   `crictl images` that it is really gone.
3. **Pull policy**: keep `IfNotPresent` (the controller's default). With the
   image evicted, IfNotPresent forces a real pull; `Always` would make every
   run a pull run and muddy the warm-start baseline.
4. **Pairs, not tags**: measure warm start (image present) and cold start
   (evicted) back-to-back on the same node, N ≥ 10 runs each, and report the
   per-phase histograms from `/metrics` rather than wall-clock anecdotes.
5. **Record the environment**: kind topology, node CPU limit, and the fact
   that registry RTT dominates `pull` locally — local numbers bound behavior
   on a LAN registry, nothing more.

The existing hot-path throughput results (`notes/optimization-results.md`)
measure a warm, ready workspace; they do not say anything about cold starts.
This document's protocol is what replaces that gap with evidence.
