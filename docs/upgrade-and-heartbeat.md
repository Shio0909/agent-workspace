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

## Agent side

`cmd/agent-runtime` serves `GET /heartbeat` with its version (`AGENT_VERSION`) and whether a turn is in flight. It is deliberately cheap and does not call the LLM.

## Trying it

`scripts/kind-lifecycle.sh` runs the scenarios on a kind cluster: heartbeat version, good upgrade, crash-on-start image with automatic rollback, unpullable image, manual rollback, and a frozen process (`SIGSTOP`) that is detected and restarted with history intact. It needs the images `agent-workspace:local`, `agent-workspace-agent:{local,v2,bad}`, and the fake LLM loaded into the cluster; see the header of the script.

## Limits

- Rollback goes back one step. There is no version history beyond `previous_image`.
- An upgrade is one-workspace-at-a-time; there is no fleet rollout or canary.
- Heartbeat only tells whether the process answers. It cannot tell whether the agent's answers are good.
- Verified on a single-node kind cluster, not on a production cluster.
