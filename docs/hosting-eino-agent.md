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
- **The entrypoint is a shell script, not an init system.** It starts PostgreSQL, runs the migrations, starts the server, and on SIGTERM stops the server first (so its final usage flush still has a database) and then PostgreSQL. If PostgreSQL dies while the server lives, the server stays up and `/health/ready` starts failing. Whether the controller then replaces the pod depends on its readiness handling; I did not test killing PostgreSQL.
- **A PostgreSQL major version is part of the image.** The data directory is version 17. Moving to 18 means a dump and restore, not an image bump.
- **One memory budget.** PostgreSQL (`shared_buffers` 64MB by default here) and the server share the profile's 1Gi. I did not load-test it.
- **Redis is optional but not quiet.** eino_agent has no switch to turn Redis off, only a default of `localhost:6379`. Without one it logs a dial failure and degrades to no cache, which adds about two seconds to each process start.

## The process runs with no capabilities

The pod runs as uid 1000 with every capability dropped, so the entrypoint does not use `gosu` or `su`: the base image is `pgvector/pgvector:pg17`, but its `postgres` user is not used, and `initdb` and `pg_ctl` run as the `agent` user. The PostgreSQL password is generated on first start and kept in `/workspace/.eino/pg_password`, mode 0600. PostgreSQL listens on `127.0.0.1` only, but a pod's loopback is shared by every container in it.

## Security notes that are specific to this profile

- eino_agent runs with `auth.enabled: false`. The gateway authenticates callers with the control token, but port 8080 of the pod is reachable by anything that can route to the pod. This repository has no NetworkPolicy ([SECURITY.md](../SECURITY.md)), so on a shared cluster that is a real gap, not a footnote.
- The key is passed to the server as an environment variable after the entrypoint reads it from the file. It is visible to anything that can read `/proc/<pid>/environ` inside the container.
- The server reads the key once. The profile therefore sets `restart_on_credential_change`, and a rotation replaces the pod (the e2e below checks this).

## Usage accounting

The meter wraps the chat model and counts what the provider reports on the response, for both `Generate` and `Stream`. It flushes additively every 5 seconds and once more on shutdown, so a graceful stop loses nothing. A hard kill loses at most the last few seconds of tokens, and the controller keeps the larger value it already recorded. A call that returns no usage (some providers omit it on a stream) is not counted; a live test confirmed that the provider used here reports usage on both call styles.

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

Not run: a PostgreSQL crash, a `kill -9` of the pod during a write, a concurrent load, knowledge-base ingestion (documents, embeddings other than the default `hash`), more than one workspace on the same cluster, a storage class other than kind's default, and anything with auth turned on inside the pod.

## Build and run

```bash
# in the eino_agent repository
docker build -f docker/Dockerfile.workspace --build-arg AGENT_VERSION=v1 -t eino-agent-workspace:local .

# in this repository: needs the image above and a real endpoint
LLM_BASE_URL=https://example.com/v1 LLM_MODEL=some-model LLM_KEY_FILE=/path/to/keyfile \
  make e2e-eino-agent KIND_CLUSTER=<a kind cluster>
```

The profile's default `LLM_BASE_URL` points at the in-cluster fake LLM so that a workspace starts without a key. Real use overrides it.
