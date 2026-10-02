# State continuity across the workspace lifecycle

The claim: one conversation session, and the workspace's token count, survive every transition that replaces the pod. This page says how each part is checked and what is only an argument.

## Where the state lives

The controller never holds session state. A workspace's PVC holds it, and the pod is disposable:

| State | Location | Written by |
| --- | --- | --- |
| Message history, compaction summaries and archives | `<volume>/sessions/...` | `cmd/agent-runtime` |
| Token usage total | `<volume>/usage.json` (outside the model's `files/` sandbox) | `cmd/agent-runtime` |
| Credentials | a Secret mounted read-only | the controller |

Stop, idle scale-to-zero, restart, upgrade and rollback only touch the Deployment (replicas, image). None of them deletes the PVC; only `DELETE` and the grace-period hard delete do. Session continuity is therefore the conjunction of two properties: the controller never removes the volume on those transitions, and the agent loads everything it needs from the volume at start.

## What is tested

| Guarantee | Check | Level |
| --- | --- | --- |
| Stop and restart never delete the PVC | `TestStopScalesToZeroAndKeepsPVC`, `TestQuantStopAndRestartNeverDeletePVC`, `TestQuantOnlyHardDeleteRemovesPVC` in `internal/kube` | unit, fake client |
| A restarted agent resumes the conversation and reuses a stored summary | `TestConversationSurvivesRestart`, `TestSummaryIsStoredAndReusedAfterARestart` in `internal/agent` | unit, real agent |
| Stop, then wake by a gateway request, keeps the session | `TestSessionSurvivesStopAndWakeOnRequest` (`internal/control/continuity_test.go`) | real controller + real agent on one volume, pod replaced |
| Idle scale-to-zero, then wake | `TestSessionSurvivesIdleScaleToZero` | same |
| Committed upgrade | `TestSessionSurvivesAnUpgradeThatIsCommitted` | same |
| Rollback of an image that never becomes ready | `TestSessionSurvivesAnUpgradeThatRollsBack` | same |
| A compacted conversation keeps its summary across stop/wake without re-summarising | `TestCompactionSummarySurvivesStopAndWake` | same |
| Lease suspension and renewal keep the session | `TestSessionSurvivesLeaseSuspensionAndRenewal` | same |
| The token count lives on the volume | `TestTokenCounterLivesOnTheVolumeNotInThePod`, `TestUsageSurvivesARestart` | same, and agent unit |
| All of the above plus upgrade/rollback and budget 402 on real Kubernetes | `scripts/kind-continuity.sh` (`make e2e-continuity`) | kind, real PVC, real Deployments |
| History is replayed after stop/start, upgrade and rollback | `scripts/kind-lifecycle.sh`, `scripts/kind-credentials.sh` (`echo(N)` counts earlier turns) | kind |

How "the session was kept" is decided matters. The fake LLM has a `recall <text>` command that answers `recall(found)` only if `<text>` appears in the earlier messages the agent sent to it. A test plants a random token (`remember <token>`), disturbs the workspace, then asks `recall <token>`. The answer comes from the model's side of the wire. A test that only compared the agent's own reply would pass on an agent that echoes. For compaction the planted word is in a turn that has been compacted away, and the test checks that the summary, not the raw turn, carries it.

The composition tests run the real controller and the real agent on one directory standing in for the PVC. The "pod" is an `httptest` server around `agent.Agent`: stopping it discards the process and nothing else. Sensitivity was checked by mutation: making the fake runtime's `Stop` wipe the volume fails five of these tests; making the agent skip persisting `usage.json` fails two.

### kind run

`make e2e-continuity KIND_CLUSTER=<name>` was run on a fresh single-node kind cluster (`kindest/node:v1.37.0-local`) with real images. Output:

```
1. explicit stop, then wake by a gateway request: new pod, same PVC; the LLM was sent the token
2. idle scale-to-zero (idle-stop in the audit log, no pod), then a request: the LLM was sent the token
3. image upgrade, committed: agent_version=v2, new pod, the LLM was sent the token
4. image upgrade that crashes on start, rolled back: serving again, the LLM was sent the token
5. usage.total_tokens=1284, billed by the LLM endpoint=1284
6. token budget 1334 -> suspended at total_tokens=1523, gateway answered 402; budget cleared: woke on the next request
continuity e2e passed
```

Step 5 is the cross-check that matters for metering: the controller's number equals the sum the LLM endpoint itself billed, after the pod had been replaced four times.

## What is only argued

- **Durability under power loss.** `usage.json` is written with temp file, fsync and rename, and session files are appended. Nothing simulates a power cut or a node failure; on a real PVC the guarantee is whatever the storage class provides. The kind PVC is local-path storage.
- **Crash in the middle of a turn.** If the agent is killed while a turn is in flight, that turn may be missing from the history, and tokens the provider billed for it may be missing from the count. Tests restart between turns, not during one. The controller never sees partial usage; the counter is persisted after each model call, so the loss is bounded by one call.
- **Other agents.** The controller side is generic: it moves a volume and a number and does not read session files. The recall guarantee is shown only for `cmd/agent-runtime`. Another workload keeps its session only if it keeps it on the volume.
- **Storage that is not durable.** The PVC survives stop because nothing deletes it. If the storage class is node-local and the node is lost, the data is lost with it. Only kind's local-path storage was exercised.
- **Upgrade with a changed on-disk format.** The tested upgrade swaps image v1 for v2 which share a format. A v2 that cannot read v1's files is a risk the controller cannot see; rollback restores the image, not the files, so a v2 that migrates the files in place may break rollback. Not tested.
- **Concurrent writers.** Two pods on one volume (for example during a rolling update) are not tested. Workspace Deployments use the Recreate strategy so the old pod stops before the new one starts; that is a design choice, not a measured result.
- **Time.** Idle scale-to-zero was checked with a 40s idle limit, not the production default.
