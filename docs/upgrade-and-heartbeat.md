# Upgrade, rollback, and heartbeat

Two features keep long-lived agent workspaces healthy without manual kubectl work: image upgrades that roll back on their own, and a heartbeat that restarts a workspace whose process is alive but stuck.

## Upgrade and rollback

```bash
# Upgrade one workspace to a new image
curl -X POST $CTRL/v1/workspaces/$ID/upgrade \
  -H "X-Control-Token: $TOKEN" -H "X-Biz-Id: up-1" \
  -d '{"image":"agent-workspace-agent:v2","timeout_seconds":120}'

# Go back one step
curl -X POST $CTRL/v1/workspaces/$ID/rollback \
  -H "X-Control-Token: $TOKEN" -H "X-Biz-Id: rb-1"
```

How it works:

1. The image must match a pattern in the profile's `allowed_images` (exact match, or a prefix ending in `*`, for example `agent-workspace-agent:*`). Profiles without the list reject upgrades.
2. The controller records the new image, remembers the old one in `previous_image`, and sets `upgrade` to `pending`. The reconciler then updates the Deployment.
3. The upgrade is committed only after the new pod has stayed ready for `-upgrade-settle`. A pod that becomes ready and then crashes does not count.
4. The clock starts at the first reconcile that wants the workspace running. If the workspace is not ready on the new image before `timeout_seconds` (default 180, allowed 10 to 1800), the controller rolls back automatically and records `last_upgrade` with outcome `rolled-back` and the reason.
5. A stopped workspace pauses the clock. It does not roll back just because nobody started it.
6. Credentials, history, and the PVC are not touched by an upgrade or rollback. The Secret and volume stay attached.

Requesting the same target twice is a no-op. Requesting a different target while an upgrade is undecided returns 409, because the fallback image would be lost. `rollback` during an undecided upgrade aborts it (outcome `aborted`). After a committed upgrade it returns to the image that ran before, and calling it again rolls forward. With no previous image it returns 409.

State is durable: a controller restart in the middle of an upgrade resumes the same decision.

`GET /v1/workspaces/{id}` shows `image`, `previous_image`, `upgrade`, and `last_upgrade`. Metrics and audit records cover each upgrade, commit, and rollback.

## Heartbeat

If the profile sets `heartbeat_path` (the agent profile uses `/heartbeat`), the controller polls that path on the workspace's service.

- A healthy reply refreshes `heartbeat_at` and records `agent_version`.
- A reply with `"busy": true` counts as activity, so a workspace in the middle of a long LLM turn is not stopped by the idle timer.
- After `-heartbeat-misses` consecutive failures the workspace is marked `unresponsive` and restarted. A restart is allowed at most once per `-restart-cooldown`, so a crash loop does not become a restart storm.
- A 4xx reply is treated as a configuration error (wrong path or auth), not as a dead process, and never triggers a restart.
- Workspaces that are not ready are only polled if they were healthy recently, so a workspace that is still starting is not restarted.

Flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-upgrade-settle` | 15s | Time a workspace must stay ready on the new image before commit |
| `-heartbeat-interval` | 10s | Spacing between polls of one workspace |
| `-heartbeat-misses` | 3 | Consecutive failures before restart |
| `-restart-cooldown` | 5m | Minimum time between heartbeat restarts of one workspace |

## Usage metering and token budget

The heartbeat reply may carry the workspace's cumulative LLM usage:

```json
{"busy": false, "version": "v1",
 "usage": {"prompt_tokens": 900, "completion_tokens": 384, "total_tokens": 1284}}
```

The field is optional, so a workload that does not report it behaves as before. The controller does not know what an LLM is: it reads three counters and a number.

Rules the controller applies:

- Cumulative and monotonic. The workload owns the counter and the controller keeps the field-wise maximum of what it has seen. The controller never adds heartbeats together, so a lost, repeated or reordered heartbeat cannot change the total.
- Each value must be an integer in [0, 2^50]. A negative, absurd or wrong-typed value rejects the whole `usage` object (logged once) but `busy` and `version` in the same reply are still honoured. Unknown fields are ignored. `total_tokens` is raised to `prompt + completion` if it is lower.
- A report below the stored value (for example a workload that lost its volume and started again from 0) is ignored, an audit event `usage-regressed` is written once per episode, and the tokens it consumed before catching up are not counted in `nc_tokens_total`. The controller cannot tell a reset from a wrong number, so it keeps the higher one.
- The total is kept in memory and flushed with the other activity fields at most once per `-activity-flush` interval (default 10s). A budget crossing is persisted at once. A controller crash can therefore lose up to one interval of usage but never the suspension.

`GET /v1/workspaces/{id}` shows `usage`, `token_budget` and, when the budget stopped it, `suspended_for: "token_budget"`.

```bash
# Allow 100k tokens; 0 removes the limit. The field is required.
curl -X PUT $CTRL/v1/workspaces/$ID/token-budget \
  -H "X-Control-Token: $TOKEN" -H "X-Biz-Id: tb-1" -d '{"token_budget":100000}'
```

`X-Biz-Id` is optional here (it sets an absolute value, so repeating it is harmless), and a replay returns the first answer. Negative values, values above 2^50 and unknown fields return 400. A scoped token can set it only for workspaces in its scope; others get the same 404 as a missing workspace.

When `total_tokens >= token_budget`:

1. The workspace is persisted as suspended (`suspended_for: "token_budget"`, `last_error` names the numbers) before anything else happens, and the audit log gets `budget-exceeded`. The next reconcile stops the workload; the volume stays.
2. Wake, `start`, `restart`, `stop` and lease on it return 402 Payment Required, not 410, so a caller can tell "renew the lease" from "raise the budget". Renewing the lease does not lift it.
3. Raising the budget above the usage, or setting 0, resumes it (audit `resume`); the workspace wakes on the next request. Raising it but not above the usage keeps it suspended. Lowering the budget to the current usage or below suspends at once.
4. The grace-period hard delete never applies to a budget suspension. A workspace is only deleted for budget reasons by an explicit `DELETE`.
5. If the lease has also expired when the budget is lifted, the workspace stays suspended for the lease.

Enforcement is by heartbeat, so a budget is a limit with some overshoot, not a hard cap: the turn in flight when the budget is crossed completes (the kind run overshot 1334 by 189 tokens). The controller also cannot refuse a request halfway through a turn.

Metrics: `nc_tokens_total{profile,kind="prompt|completion"}` and `nc_budget_suspensions_total{profile}`. There is no workspace label, to keep cardinality bounded; per-workspace numbers are in the API. Counters are process-local and restart at 0.

`cmd/agent-runtime` implements the reporting side. It counts the `usage` object of every chat-completion response, including summarisation calls and responses it could not use, because the provider billed them. It stores the total at `<workspace dir>/usage.json`, outside the directory the model's file tools can write, so a pod replacement keeps it. If a provider omits `usage` the call adds nothing: estimating from text length would be a number nobody can reconcile with an invoice. A 401 costs nothing. A corrupt `usage.json` restarts the count from 0 and the controller keeps its higher recorded value.

## Agent side

`cmd/agent-runtime` serves `GET /heartbeat` with its version (`AGENT_VERSION`), whether a turn is in flight, and its cumulative token usage. It is deliberately cheap and does not call the LLM.

## Trying it

`scripts/kind-lifecycle.sh` runs the scenarios on a kind cluster: heartbeat version, good upgrade, crash-on-start image with automatic rollback, unpullable image, manual rollback, and a frozen process (`SIGSTOP`) that is detected and restarted with history intact. It needs the images `agent-workspace:local`, `agent-workspace-agent:{local,v2,bad}`, and the fake LLM loaded into the cluster; see the header of the script.

`scripts/kind-continuity.sh` (`make e2e-continuity`) checks that a conversation and the token count survive stop, idle scale-to-zero, upgrade and rollback, and that a budget suspends the workspace with a 402 and resumes it. See [state-continuity.md](state-continuity.md).

## Limits

- Rollback goes back one step. There is no version history beyond `previous_image`.
- An upgrade is one-workspace-at-a-time; there is no fleet rollout or canary.
- Heartbeat only tells whether the process answers. It cannot tell whether the agent's answers are good.
- Verified on a single-node kind cluster, not on a production cluster.
- Token usage is only as good as the workload's report. A workload that lies about usage can evade its budget; the budget limits cost, it is not a sandbox.
