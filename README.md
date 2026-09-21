# agent-workspace

A small Kubernetes controller for agent workspace lifecycle.

It manages one Deployment, Service, and PersistentVolumeClaim per workspace. Workspaces can be started on demand, stopped when idle, resumed with their storage intact, and reclaimed in explicit stages.

## Features

- Per-workspace lifecycle: create, start, stop, restart, and delete.
- Lazy wake-up through the HTTP gateway.
- Idle scale-to-zero with PVC retention.
- Three-stage reclamation: idle stop, suspension, and hard delete after a grace period.
- Idempotent lifecycle operations using caller-provided business IDs.
- Durable intent and reconciliation across controller restarts.
- Bounded reconciliation concurrency and per-workspace serialization.
- Readiness-aware HTTP and SSE/WebSocket forwarding.
- Audit trail and Prometheus-compatible metrics.

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
   +--> local JSON snapshot
   +--> audit log
   +--> metrics
```

The controller records intent before touching Kubernetes. Reconciliation is idempotent, serialized per workspace, and safe to retry after a restart.

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
| `GET` | `/v1/audit` | Query lifecycle audit records |
| `ANY` | `/w/{id}/*` | Forward HTTP, SSE, and WebSocket traffic |

Lifecycle writes require `X-Biz-Id` for idempotency. All control-plane endpoints require `X-Control-Token`.

## Development

```bash
make test
make vet
make build
```

See [docs/control-plane.md](docs/control-plane.md) for the state machine and invariants.

## Security

This project uses a single shared control token and targets a single-controller deployment. It is not a multi-tenant sandbox. See [SECURITY.md](SECURITY.md).

## Benchmarks

Local kind benchmark methodology and saturation results are in [docs/benchmarks.md](docs/benchmarks.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
