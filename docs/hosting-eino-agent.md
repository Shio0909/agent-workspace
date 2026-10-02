# Hosting eino_agent as a workspace profile

`eino-agent` is the second real agent this controller runs, after the small `agent` runtime in this repository. eino_agent is a Go RAG service (Gin, Eino, PostgreSQL with pgvector). It was written with no knowledge of this controller, so it shows what the controller's contract costs an agent that was not built for it.

The image is built from the eino_agent repository (`docker/Dockerfile.workspace`), not from this one. This repository only carries the profile in `configs/profiles.json`, the e2e script `scripts/kind-eino-agent.sh`, and this page.

## What the agent had to provide

| Contract | What eino_agent does |
| --- | --- |
| A port and a readiness path | `8080`, `GET /health/ready`. It probes the database and the vector store (the same PostgreSQL here) and reports 200 only when both answer; Redis is optional |
| State on the volume | Its own PostgreSQL data directory, `/workspace/pgdata`, plus `/workspace/data` for its audit log |
| A credential as a file | The entrypoint reads `llm_api_key` from `/var/run/agent-credentials` once, at start |
| A heartbeat | `GET /heartbeat` returns `{busy, version, usage}`. `busy` counts chat requests in flight (a streamed reply counts until the stream ends), `version` is `AGENT_VERSION` (the image tag), `usage` is the cumulative provider-reported tokens |

None of this is a change to the controller. The two changes in eino_agent that exist for it are the heartbeat endpoint and the usage meter, and the meter is useful without a controller: it persists a running total in a one-row table, `llm_usage_totals`.

## Where PostgreSQL runs

The profile model is one container and one PVC. The options I weighed:

| Option | What it needs from the controller | Verdict |
| --- | --- | --- |
| **PostgreSQL in the same container, data on the PVC** | Nothing | Chosen |
| A second container (sidecar) in the pod, data on the same PVC | A pod template with more than one container, a volume shared between them, readiness over both | Cleaner process model, but the controller's pod spec, upgrade and rollback logic all assume one container |
| One shared PostgreSQL, one database per workspace | A provisioning step per workspace, credentials per database, deletion that drops the database | Makes cross-workspace queries possible, and makes the platform own a schema lifecycle. Stop, wake, upgrade and GC would all need a second place to look |
| A PostgreSQL operator | A new dependency in the cluster | More than a single-tenant workspace needs |

What the choice costs, so nobody has to find out later:

- **No HA, no backup beyond the PVC.** Losing the PVC loses the sessions. This is the same promise the `agent` profile makes.
- **The entrypoint is a shell script, not an init system.** It starts PostgreSQL, runs the migrations, starts the server, and on SIGTERM stops the server first (so its final usage flush still has a database) and then PostgreSQL. `pg_ctl` daemonizes PostgreSQL, so the shell has no child to wait on; a small watcher checks the postmaster every two seconds (a zombie counts as dead) and, when it is gone, stops the server and exits non-zero. The container then restarts and PostgreSQL replays its WAL. Before the watcher existed the server stayed up with every query failing and recovery depended on the controller noticing the lost heartbeat, which took a minute and a pod replacement. A crashed backend that the postmaster survives is not treated as a death: PostgreSQL restarts its own backends.
- **A PostgreSQL major version is part of the image.** The data directory is version 17. Moving to 18 means a dump and restore, not an image bump.
- **One memory budget.** PostgreSQL (`shared_buffers` 64MB by default here) and the server share the profile's 1Gi. Six concurrent chat turns peaked at about 78MB in the cgroup's `memory.peak` (which includes page cache) with no OOM kill, so the limit has a wide margin for this workload. Document ingestion of large files was not measured.
- **Redis is off.** The image has none, and eino_agent used to dial `localhost:6379` whenever `redis.addr` was empty, which cost a connection timeout on every start. `redis.disabled: true` in `config.workspace.yaml` skips the attempt.

## The process runs with no capabilities

The pod runs as uid 1000 with every capability dropped, so the entrypoint does not use `gosu` or `su`: the base image is `pgvector/pgvector:pg17`, but its `postgres` user is not used, and `initdb` and `pg_ctl` run as the `agent` user. The PostgreSQL password is generated on first start and kept in `/workspace/.eino/pg_password`, mode 0600. PostgreSQL listens on `127.0.0.1` only, but a pod's loopback is shared by every container in it.

## Security notes that are specific to this profile

- eino_agent runs with `auth.enabled: false`. The gateway authenticates callers with the control token, but port 8080 of the pod is reachable by anything that can route to the pod. This repository has no NetworkPolicy ([SECURITY.md](../SECURITY.md)), so on a shared cluster that is a real gap, not a footnote.
- The key is passed to the server as an environment variable after the entrypoint reads it from the file. It is visible to anything that can read `/proc/<pid>/environ` inside the container.
- The server reads the key once. The profile therefore sets `restart_on_credential_change`, and a rotation replaces the pod (the e2e below checks this).

## Usage accounting

The meter wraps the chat model and counts what the provider reports on the response, for both `Generate` and `Stream`. It flushes additively when a request ends, every 5 seconds, and once more on shutdown, so a graceful stop loses nothing. A hard kill (SIGKILL of the pod, or PostgreSQL dying) can only lose the tokens of a turn that was still running: the fault run below killed PostgreSQL and then the whole pod right after a turn and found everything committed. Before the flush-on-request-end change the same test lost one whole turn, 13,423 tokens, because the kill came inside the 5-second window; the controller kept the larger value it had recorded and audited `usage-regressed`, as designed. A call that returns no usage (some providers omit it on a stream) is not counted; a live test confirmed that the provider used here reports usage on both call styles.

The number is what the provider said it billed for calls made by this process. It does not include a call that was cut off before the provider returned its usage.

## What was run, and what was not

All of it used a real OpenAI-compatible endpoint (SenseAudio, `qwen3.8-27b`), not the fake LLM. The fake LLM cannot tell you whether a real provider reports usage, or whether a real model can find a token in the history a session replays, so it was not used here.

`scripts/kind-eino-agent.sh` ran to the end on a fresh single-node kind cluster, from an image built on the same machine. Each step plants a random token (`zx-` plus 12 hex digits) in one session, disturbs the workspace, and asks the model for it. The model's answer must equal the token.

| Step | Result |
| --- | --- |
| Explicit stop, wake by a gateway request | New pod, same PVC, token recalled |
| Idle scale-to-zero (60s), wake by a request | `idle-stop` audited, no pod, new pod, token recalled |
| Image upgrade `local` to `v2`, committed | `agent_version=v2` came from the pod's `/heartbeat`, token recalled |
| Image upgrade to an image that exits at once | Rolled back after 40s, serving `v2` again, token recalled |
| Credential rotation | A wrong key made model calls fail (HTTP 500 carrying the provider's 401). Both key changes replaced the pod. With the right key the session was intact and the token recalled |
| Usage | Controller `usage.total_tokens` (64212) equalled the pod's own `/heartbeat` count after four pod replacements. No `usage-regressed` audit |
| Token budget | Set 50 tokens above the current count. The workspace was suspended after the next turn, the gateway answered 402, and clearing the budget woke it with the earlier token still recalled |

Two things in that table need a caveat:

- **The usage check compares the controller with the pod, not with the provider.** Both numbers come from the provider's own usage field as reported to eino_agent, so the check shows the controller and the pod agree and that the count survives replacement. It does not show that the provider's invoice matches: I did not find a provider-side usage query to compare against, so that is unchecked.
- **A budget is a limit checked after a turn, not before.** The budget was 64262 and the workspace was suspended at 69023. One turn of this agent costs about 4,300 prompt tokens (its system prompt and tool definitions), so the overshoot is up to one turn plus whatever runs concurrently with it.

Before the kind run, the same image ran under plain Docker with uid 1000, `--cap-drop ALL`, `no-new-privileges`, a 1GiB memory limit and a volume: `docker stop` took one second and exited 0, and after `docker start` the session and the usage count (8637 tokens by then) were still there. The server also ran natively against a scratch database with a graceful restart in between, with the same outcome.

### Failure and load: `scripts/kind-eino-agent-faults.sh`

Same cluster, same image, same real model, one run end to end (`make e2e-eino-agent-faults`). Every step has an assertion; the numbers below are from the passing run.

| Step | Result |
| --- | --- |
| Streamed answer through the gateway | First content event after 5.55s of 7.88s, 219 content events. Before the streaming fix eino_agent sent the whole answer in one event at the end |
| PostgreSQL killed with SIGKILL, 6s after a turn | The watcher stopped the server, the container restarted in the same pod, and the workspace answered again 5s after the kill. The session was intact and the model repeated the token. Committed usage equalled the pod's count (0 unflushed), no `usage-regressed` |
| PostgreSQL killed right after a turn | Answered again 17s after the kill. The kubelet's back-off for a second quick crash and the controller's heartbeat restart overlapped, so the pod was replaced. Session intact, 0 tokens unflushed, no `usage-regressed` |
| Pod deleted with `--grace-period=0` (no clean PostgreSQL stop) | Replacement pod, session recovered, token repeated. What the pod had committed (54001) was still there (63266 after one more turn). The controller's count never went down |
| Six concurrent turns | All six returned 200; `memory.peak` 77.7MB, `oom_kill` 0, no container restart |
| Knowledge base: create, upload a document with a planted passphrase, ask | 1 chunk, default `hash` embedding. The agent found the passphrase (`supported_by_retrieval`, `evidence_count` 1) |
| A second workspace in the same namespace | Own PVC. It could not see the first workspace's session or token. Usage counts 98331 and 8734, tracked separately |

What this does not cover, and the limits of what it does:

- **Embeddings other than `hash`.** The knowledge-base step used the default hash embedding, so it shows that ingestion and retrieval work end to end, not that semantic retrieval is good. A real embedding provider needs its own key and was not used.
- **The provider's invoice.** Usage numbers are what the provider reported to eino_agent. No provider-side query was available to compare.
- **Load.** Six concurrent turns, one workspace. Not sustained load, not many workspaces, and not large document ingestion.
- **Crash consistency beyond what PostgreSQL promises.** `kill -9` of PostgreSQL and a force-deleted pod are process and pod deaths. A node losing power, or a storage class that acknowledges writes it has not made durable, was not tested.
- **The kind defaults.** One node, the default storage class, no NetworkPolicy enforcement checked, auth off inside the pod.
- **Timing is host-measured.** In one local image test the container's log timestamps disagreed with the elapsed time measured on the host by about 45 seconds (the VM's clock, not the service), so durations here come from the host, not from log timestamps.

## Build and run

```bash
# in the eino_agent repository
docker build -f docker/Dockerfile.workspace --build-arg AGENT_VERSION=v1 -t eino-agent-workspace:local .

# in the eino_agent repository: no LLM or cluster needed, only Docker. Checks start, a
# knowledge base surviving a PostgreSQL SIGKILL (the container must restart by itself),
# and a clean stop.
scripts/workspace-image-smoke.sh

# in this repository: needs the image above and a real endpoint
LLM_BASE_URL=https://example.com/v1 LLM_MODEL=some-model LLM_KEY_FILE=/path/to/keyfile \
  make e2e-eino-agent KIND_CLUSTER=<a kind cluster>          # lifecycle
  make e2e-eino-agent-faults KIND_CLUSTER=<a kind cluster>   # failure and load
```

The profile's default `LLM_BASE_URL` points at the in-cluster fake LLM so that a workspace starts without a key. Real use overrides it.
