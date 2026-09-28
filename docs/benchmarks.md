# Benchmarks

These measurements are local `kind` comparisons, not production capacity
claims. The goal is to compare implementations and expose the next bottleneck.

## HTTP forwarding path

Environment:

- macOS arm64, Docker/Colima, single-node kind
- controller -> Kubernetes Service -> demo workspace
- target: demo `/health`
- keep-alive client, 3 rounds x 10 seconds

| Variant | Throughput | P95 | Errors |
| --- | ---: | ---: | ---: |
| Serial readiness probe per request | about 38 req/s | about 600 ms | 0 |
| 5 s ready-address cache | 850+ req/s | about 30 ms | 0 |

The cache is invalidated on start/stop changes and proxy connection or HTTP 503
failures. The benchmark is limited to the local demo HTTP path; it does not
measure LLM inference or production network behavior.

## Extended saturation run

A second run used the controller-to-demo forwarding path for 60 seconds.

| Concurrency | Requests | Throughput | P99 | Errors |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 48,511 | 807 req/s | 346 ms | 0 |
| 200 | 28,814 | 476 req/s | 699 ms | 0 |

During the 200-concurrency run, the controller used about 0.2 CPU cores and
about 55 MiB of memory. Throughput therefore saturated before CPU or memory
did. The next identified bottleneck is the per-request state snapshot:
`Acquire` and `release` both persist activity to the local JSON snapshot,
which performs a synchronous file write and `fsync`.

The follow-up moved activity counters out of the request fsync path and added a
pooled upstream transport. A later local branch also added an informer-backed
Kubernetes read cache and event-driven reconciliation. In a two-node kind run
with the ready-address cache disabled to expose the runtime read path, baseline
performed Deployment GETs during steady state while the informer variant
performed none; the remaining discovery GET came from the controller readiness
probe. The forced-observation throughput comparison is intentionally not
published as a product number because disabling the ready cache is a
deliberately degraded configuration.

The same run added cold-start histograms for schedule, pull, ready, and total.
Kubernetes condition timestamps are commonly second-granular, so phase sums can
be coarse even though the total uses the controller's real clock. Raw local
artifacts are intentionally excluded from the repository.

## Idempotency

- Same `biz_id` across 64 concurrent requests: 1 side effect.
- Control removed: 64 concurrent requests produce 64 side effects.
- Replaying an old successful request does not revive an already stopped
  workspace.

## Storage reclamation

- Suspended workspace across 50 reconciliation rounds: 0 delete calls.
- Grace expiry: exactly 1 hard delete.
- Stop and restart paths: PVC is never deleted.

## Method notes

- Same kind cluster and cached demo image for before/after comparisons.
- Results include failed experiments rather than deleting them.
- Benchmarks exercise the storage and HTTP control path, not model inference.
