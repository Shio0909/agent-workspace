# Related projects

Projects that also run long-lived agents on Kubernetes, and how they split the work between the platform and the agent. This page is a reading list with a verdict on what each one means for this repository. It is not a benchmark, and I did not install or run any of them.

What I actually read, so you can weigh the claims:

| Project | What I read |
| --- | --- |
| Agent Sandbox | Its public docs; see [comparison.md](comparison.md) |
| kagent | The repository README (architecture section and feature list) |
| OpenClaw operators | The top of `openclaw-rocks/k8s-operator`'s README (CRD example, feature table). The forks listed below I know only from search results |
| Everything under "Seen by title only" | A one-line search result. No claim about them is made beyond that line |

## The design question they answer differently

Every project here has to decide **who owns the state of a conversation**: the platform, or the agent. The answers fall on a line.

| | State lives | The platform needs to know about the agent? |
| --- | --- | --- |
| agent-workspace (`agent` and `eino-agent` profiles) | On the workspace's PVC, written by the agent | Only: a port, a health path, optionally a heartbeat. It never reads session files |
| OpenClaw operators | On the instance's PVC | Yes. The CRD is OpenClaw's: skills, plugins, config merge modes, self-configure |
| Agent Sandbox | On an optional persistent volume of the sandbox pod | No. It orchestrates a pod and leaves the contents alone |
| kagent | In PostgreSQL, outside the running agent (per its README: sessions, tasks and history "persist independently of the running agents") | Yes. `Agent` and `ModelConfig` resources, A2A and MCP APIs, and a Harness choosing the runtime |

Putting state in the volume keeps the platform small and agent-agnostic, and costs you cross-workspace queries: you cannot ask "which sessions mention X" without asking each workspace. Putting it in a shared database makes that easy and makes the platform responsible for the schema. The `eino-agent` profile is the volume approach with a twist: the agent's own PostgreSQL runs in the same container, with its data directory on the volume ([hosting-eino-agent.md](hosting-eino-agent.md)).

## kagent

A CNCF project: a controller that watches `Agent`, `ModelConfig` and `RemoteMCPServer` resources, a UI and CLI, PostgreSQL for sessions and tasks, and a component called Substrate that runs agents and "manages their lifecycle, including suspension, resumption, and snapshots". Agents run on the Go or Python ADK, Codex, Claude or a custom runtime. It has a `SandboxAgent` resource that builds on Agent Sandbox.

What it means here:

- It is a platform for **declaring agents**; `agent-workspace` is a platform for **keeping a process alive and reachable**. Nothing in this repository defines what an agent is, and that is deliberate.
- Its suspension and resumption is the same idea as stop and wake here, implemented below the agent (snapshots) instead of beside it (a volume that outlives the pod). I have not read how Substrate does it, so I can not say which survives a mid-turn kill better.
- Its sessions survive because they are in a database. Here they survive because [the controller never deletes the volume](state-continuity.md). The tests in this repository say nothing about kagent.

## OpenClaw operators

Several operators target OpenClaw, an agent product. The one I read (`openclaw-rocks/k8s-operator`, also published as `paperclipinc/openclaw-operator`; `eqtylab`, `w9n` and `alessandrolomanto` have repositories of the same shape by their search results) turns one `OpenClawInstance` resource into a StatefulSet, Service, PVC, NetworkPolicy, PodDisruptionBudget and more. Its feature table lists: scale-to-zero with `spec.suspended`, opt-in auto-update that backs up, rolls out and rolls back on failed health checks, config applied from the resource on each restart with a `merge` mode that keeps runtime changes on the volume, and an agent "self-configure" path that lets the agent install skills through the Kubernetes API under an allowlist.

What it means here:

- It covers much of what `agent-workspace` covers for one concrete agent (suspend, update with rollback, state on a volume) and adds things this repository does not have: default-deny NetworkPolicy, a validating webhook, HPA, config rendering. It pays for that by being specific to OpenClaw.
- The default-deny network policy is the most useful idea to borrow. This repository's pods can reach anything the cluster allows; see the egress item in [SECURITY.md](../SECURITY.md).

## Agent Sandbox

Covered in [comparison.md](comparison.md). Short version: it is about isolation and a Kubernetes-native API for a single stateful pod; it does not do credentials, upgrades or metering.

## Seen by title only

I did not read these. They are listed because the search for similar projects returned them, and a reader may want to look.

- `k8s4claw`: described in its post as a Kubernetes operator that deploys heterogeneous agent runtimes from one CRD.
- `aws-samples` multi-tenancy OpenClaw on EKS.
- `fastclaw-ai/clawhost`.
- Agent Substrate, which kagent's README names as the thing that runs its agents.

## What was borrowed or rejected because of this reading

- **Not borrowed:** CRDs. A bbolt store and an HTTP API with idempotency keys is a different trade; see the first row of the table in [comparison.md](comparison.md).
- **Worth doing, not done:** a default-deny egress policy per workspace, allowing the LLM endpoint and the control plane only.
- **Already shared:** state on a volume that outlives the pod, suspension that keeps the volume, rollback on a failed update.
