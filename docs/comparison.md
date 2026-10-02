# Comparison with Agent Sandbox

[Agent Sandbox](https://agent-sandbox.sigs.k8s.io/docs/)
(`kubernetes-sigs/agent-sandbox`, under Kubernetes SIG Apps) and
`agent-workspace` both manage long-lived, stateful, one-pod-per-agent
environments on Kubernetes. They overlap less than that sentence suggests.

This page is a reading of Agent Sandbox's public documentation next to this
repository's code. I did not install or run Agent Sandbox, so every statement
about it is what its docs say, not something measured here. It contains no
benchmark numbers for either project.

## In one paragraph

Agent Sandbox is a **sandbox orchestrator**: a set of CRDs (`Sandbox`,
`SandboxTemplate`, `SandboxClaim`, `SandboxWarmPool`) whose job is a
declarative, standard way to get an isolated, stateful, singleton pod, with the
isolation itself delegated to a runtime such as gVisor or Kata Containers.
`agent-workspace` is a **lifecycle controller with an HTTP API**: it decides
when a workspace runs, sleeps and is reclaimed, forwards traffic to it, and
manages what an agent needs while it lives (credentials, upgrades, health). It
provides no isolation of its own and says so in [SECURITY.md](../SECURITY.md).

## At a glance

| | Agent Sandbox (per its docs) | agent-workspace |
| --- | --- | --- |
| Interface | Kubernetes CRDs reconciled by a controller; Go and Python SDKs | HTTP API with idempotent writes (`X-Biz-Id`); state in bbolt; no CRDs |
| Unit managed | One pod with a stable hostname and optional persistent storage | One Deployment (0 or 1 replica, `Recreate`), Service and PVC per workspace |
| Isolation | Delegated to a `RuntimeClass` (gVisor, Kata); aimed at untrusted, LLM-generated code | None beyond container hardening (all capabilities dropped, no privilege escalation, no service account token). Not a sandbox |
| Fast provisioning | `SandboxWarmPool` pre-warms pods; `SandboxClaim` takes one | No warm pool. Cold start is measured (histogram) and paid on first request |
| Idle handling | Pause and resume; the docs describe automatic resume on network connections | Idle scale-to-zero keeping the PVC; woken by the first gateway request or an explicit start |
| Expiry | Scheduled deletion (TTL) | Three stages: idle stop, suspension when the lease expires, hard delete after a grace period. Expired workspaces answer 410 until renewed |
| Traffic | Stable hostname; optional Sandbox Router for SDK clients | Built-in gateway at `/w/{id}/*` for HTTP, SSE and WebSocket, readiness-aware, wakes a stopped workspace |
| Credentials | Not a feature in the docs I read | Per-workspace Secret mounted as files with a version; rotation without a pod restart |
| Upgrades | Not covered in the docs I read | Allowed-image list, automatic rollback if the new image does not become ready, heartbeat that restarts a stuck workspace |
| Authorization | Kubernetes RBAC, namespaces, network policies, quotas | Shared control token plus optional tokens scoped to workspace ids or profiles |
| Availability | Not covered in the docs I read | One controller replica, no leader election |
| Maturity | SIG Apps project with versioned releases | A reference implementation |

## What Agent Sandbox has that this does not

- **Isolation depth.** Running untrusted code safely is its stated purpose, and
  the way it gets there (a sandbox runtime behind `RuntimeClass`) is the right
  one. Nothing here substitutes for it.
- **Warm pools.** Allocation in milliseconds instead of a cold pod start.
- **A Kubernetes-native API.** `kubectl`, GitOps, RBAC and admission policy all
  work on `Sandbox` objects. This project's state is in a bbolt file that only
  its own API can read.
- **SDKs and a community.** Go and Python clients, a router, a roadmap and
  maintainers.

## What this has that the Sandbox docs do not describe

These are lifecycle concerns of an agent *product*, not of a sandbox:

- **Credential rotation.** An agent's LLM key can be replaced while a
  conversation is in flight, and the failing version is named in the error
  ([agent-credentials.md](agent-credentials.md)).
- **Upgrade with rollback and a heartbeat.** A bad image is rolled back
  automatically; a hung process is restarted
  ([upgrade-and-heartbeat.md](upgrade-and-heartbeat.md)).
- **Staged reclamation with invariants.** Storage is never deleted by the idle
  or suspension step, only by an explicit delete or the end of a grace period
  ([control-plane.md](control-plane.md), section 3).
- **Idempotent, audited lifecycle writes**, so a caller can retry a create or a
  delete after a timeout without knowing whether the first attempt landed.

The Sandbox docs I read do not rule these out; they are outside what a sandbox
orchestrator sets out to do, so an Agent Sandbox user would build or buy them
separately.

## Choosing

| If you need | Use |
| --- | --- |
| To run code you do not trust, or tenants that must not see each other | Agent Sandbox with a sandbox runtime |
| Sub-second allocation of fresh environments (evaluation, RL loops) | Agent Sandbox with a warm pool |
| A declarative, GitOps-friendly resource | Agent Sandbox |
| One trusted team's agents, each with its own volume, an HTTP API, a built-in gateway, key rotation and staged cleanup, in a single small binary | `agent-workspace` |
| To study how such a controller is built (durable intent, idempotency, reconciliation, bounded concurrency) | `agent-workspace` |

## Using them together

The controller reaches Kubernetes only through the `Runtime` interface in
[`internal/control/model.go`](../internal/control/model.go) (`Ensure`,
`Observe`, `Stop`, `Delete`, `Restart`, `Probe`). A runtime that creates
`Sandbox` objects instead of Deployments would keep the HTTP API, gateway,
credentials and reclamation policy and add the isolation. That is a possible
direction, not something that exists: the Deployment-based runtime is the only
one, and mapping stop and wake-up onto Sandbox pause and resume would need
checking against what the Sandbox controller actually guarantees.

## The workload

Agent Sandbox runs whatever image you give it. `cmd/agent-runtime` here is a
small ReAct agent that exists so the controller manages a real agent: it reads
its key from the credential mount, keeps conversations on the workspace volume,
confines its file tools to one directory, and compacts a session that no longer
fits the model's context window into a summary
([`internal/agent/compact.go`](../internal/agent/compact.go); set
`AGENT_CONTEXT_TOKENS` to turn it on). It is a sample workload, not a product.

## Sources

Read on 2026-10-02:

- <https://agent-sandbox.sigs.k8s.io/docs/> (documentation index)
- <https://agent-sandbox.sigs.k8s.io/docs/getting_started/overview/> (overview,
  architecture, motivation; the install snippet there pins `v1.0.2` and the
  example uses `apiVersion: agents.x-k8s.io/v1beta1`)

Where the overview lists a capability under "desired characteristics" (deep
hibernation, memory sharing across sandboxes), it is a goal of that project and
is not treated here as shipped.
