# agent-workspace

A small Kubernetes controller for agent workspace lifecycle.

It manages one Deployment, Service, and PersistentVolumeClaim per workspace. Workspaces can be started on demand, stopped when idle, resumed with their storage intact, and reclaimed in explicit stages.

## Where it fits

This project decides when a workspace runs, sleeps, and is reclaimed, forwards traffic to it, and handles what an agent needs while it lives: credentials, upgrades, health. It does not isolate untrusted code. If that is the requirement, use a sandbox runtime, for example through [Agent Sandbox](https://agent-sandbox.sigs.k8s.io/docs/) (`kubernetes-sigs/agent-sandbox`), which is built for that and also offers warm pools and a Kubernetes-native API. [docs/comparison.md](docs/comparison.md) sets the two side by side and says what each does not do.

## Features

- Per-workspace lifecycle: create, start, stop, restart, and delete.
- Lazy wake-up through the HTTP gateway.
- Idle scale-to-zero with PVC retention.
- Three-stage reclamation: idle stop, suspension, and hard delete after a grace period.
- Idempotent lifecycle operations using caller-provided business IDs.
- Durable intent and reconciliation across controller restarts.
- Event-driven reconciliation with a rate-limited retry queue.
- Kubernetes informer cache for managed workloads, with direct API fallback.
- Bounded reconciliation concurrency and per-workspace serialization.
- Readiness-aware HTTP and SSE/WebSocket forwarding.
- Audit trail, cold-start histograms, and Prometheus-compatible metrics.
- Per-workspace credential injection and rotation: the LLM key lives in a Kubernetes Secret mounted as files, so an agent picks up a new key without a pod restart. See [docs/agent-credentials.md](docs/agent-credentials.md).
- Image upgrade with automatic rollback, and a heartbeat that restarts a stuck workspace. See [docs/upgrade-and-heartbeat.md](docs/upgrade-and-heartbeat.md).

## Architecture

```text
HTTP API
   |
   v
Controller state machine
   |
   +--> client-go Kubernetes runtime
   |       |
   |       +--> Deployment
   |       +--> Service
   |       +--> PersistentVolumeClaim
   |
   +--> bbolt state database
   +--> audit log
   +--> metrics
```

The controller records intent before touching Kubernetes. Reconciliation is idempotent, serialized per workspace, and safe to retry after a restart. Periodic reconciliation is the backstop; runtime events and API intent changes enter a deduplicating event queue for low-latency updates.

## Quick Start

Requirements:

- Go 1.25+
- Docker
- kind
- kubectl
- A Kubernetes cluster with a default StorageClass for the full lifecycle demo

Build and run locally:

```bash
make test
make vet
make build
make images
```

Kind e2e:

```bash
kind create cluster --name agent-workspace
make e2e
```

`make e2e` builds the images, loads them into kind, deploys the controller, creates a demo workspace, verifies health and persistent writes, and removes the test namespace.

## API

The controller exposes a small HTTP API.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Kubernetes API readiness |
| `GET` | `/metrics` | Prometheus-compatible metrics |
| `GET` | `/v1/workspaces` | List workspaces |
| `POST` | `/v1/workspaces` | Create a workspace |
| `GET` | `/v1/workspaces/{id}` | Get workspace state |
| `POST` | `/v1/workspaces/{id}/start` | Start a workspace |
| `POST` | `/v1/workspaces/{id}/stop` | Stop a workspace while keeping storage |
| `POST` | `/v1/workspaces/{id}/restart` | Restart the workload |
| `DELETE` | `/v1/workspaces/{id}` | Delete the workload and storage |
| `PUT` | `/v1/workspaces/{id}/credentials` | Replace the workspace's credentials (returns version and key names only) |
| `GET` | `/v1/workspaces/{id}/credentials` | Credential metadata, never values |
| `DELETE` | `/v1/workspaces/{id}/credentials` | Remove the workspace's credentials |
| `POST` | `/v1/workspaces/{id}/upgrade` | Move to an allowed image; rolls back automatically if it does not become ready |
| `POST` | `/v1/workspaces/{id}/rollback` | Return to the previous image |
| `PUT` | `/v1/workspaces/{id}/token-budget` | Set the workspace's LLM token budget (`0` = unlimited); at the budget it is suspended and the gateway answers 402 |
| `GET` | `/v1/audit` | Query lifecycle audit records |
| `ANY` | `/w/{id}/*` | Forward HTTP, SSE, and WebSocket traffic |

Lifecycle writes require `X-Biz-Id` for idempotency. All control-plane endpoints require `X-Control-Token`. The shared token (`AGENT_WORKSPACE_TOKEN`) can do everything; with `-tokens` you can also issue tokens limited to some workspace ids or profiles, so callers cannot see or touch each other's workspaces (see [docs/control-plane.md](docs/control-plane.md), section 11).

## Agent workload

`cmd/agent-runtime` is a small ReAct agent that runs in a workspace: it reads its LLM key from the mounted credential directory, keeps conversations on the workspace volume, and confines its file tools to one directory. Set `AGENT_CONTEXT_TOKENS` to the model's context window and a session that outgrows it is compacted: older turns become a summary, recent turns stay verbatim, and a failed summary falls back to a raw archive (`AGENT_RESERVE_TOKENS` and `AGENT_KEEP_RECENT_TOKENS` tune the budgets; see `internal/agent/compact.go`). `cmd/fake-llm` is a deterministic OpenAI-compatible endpoint for demos and tests. The `agent` profile in `configs/profiles.json` wires them together; `scripts/kind-credentials.sh` runs the rotation scenario end to end and `scripts/kind-lifecycle.sh` the upgrade, rollback and heartbeat scenarios; `make e2e-continuity` checks that one conversation and its token count survive stop, idle scale-to-zero, upgrade and rollback (see [docs/state-continuity.md](docs/state-continuity.md)). The agent reports its cumulative LLM token usage in the heartbeat and the controller enforces an optional per-workspace budget (see [docs/upgrade-and-heartbeat.md](docs/upgrade-and-heartbeat.md)). `cmd/agent-eval` and `scripts/llm-trial.sh` run repeated trials against any OpenAI-compatible endpoint. The `eino-agent` profile hosts a third-party Go RAG service with its own PostgreSQL in one container; `make e2e-eino-agent` runs the same lifecycle against a real model and `make e2e-eino-agent-faults` kills PostgreSQL and the pod, runs concurrent turns, and checks a knowledge base and a second workspace (see [docs/hosting-eino-agent.md](docs/hosting-eino-agent.md)). Workspace pods meet the Pod Security `restricted` profile and accept traffic only from the controller (a NetworkPolicy in `deploy/controller.yaml`); `make e2e-isolation` enforces the profile on a namespace and checks both (see [SECURITY.md](SECURITY.md)). Other projects that run agents on Kubernetes are listed in [docs/related-projects.md](docs/related-projects.md).

## Development

```bash
make test
make vet
make build
```

See [docs/control-plane.md](docs/control-plane.md) for the state machine and invariants.

## Security

This project uses a shared control token, optionally narrowed per caller with scoped tokens, and targets a single-controller deployment. It is not a multi-tenant sandbox. See [SECURITY.md](SECURITY.md).

## Benchmarks

The local kind benchmark method is described in [docs/benchmarks.md](docs/benchmarks.md); run `scripts/benchmark-kind.sh` for your own numbers.

## License

Apache-2.0. See [LICENSE](LICENSE).
