#!/usr/bin/env bash
# End-to-end check that one conversation session keeps its context, and the
# workspace keeps its token count, across the whole workspace lifecycle, with
# the real controller, real agent pods and the fake LLM.
#
# Each step plants a random token in the session "cont" ("remember <token>"),
# disturbs the workspace, then asks "recall <token>". The fake LLM answers
# "recall(found)" only if the token is in the message history the agent sent
# to it, so the answer comes from the model's side of the wire, not from the
# agent's own reply.
#
#   1. explicit stop, then wake by a gateway request
#   2. idle scale-to-zero (no request, no stop call), then wake by a request
#   3. image upgrade that is committed
#   4. image upgrade that never becomes ready and is rolled back
#   5. the controller's token count equals what the LLM endpoint billed, even
#      though the pod was replaced four times
#   6. a token budget suspends the workspace, the gateway refuses it with 402,
#      and raising the budget brings it back
#
# Images that must be loaded into the cluster first (make e2e-continuity does
# this): agent-workspace:local, agent-workspace-agent:{local,v2,bad},
# agent-workspace-fakellm:local. See scripts/kind-lifecycle.sh for how they are
# built.
set -euo pipefail

namespace="${NAMESPACE:-agent-workspace}"
cluster="${KIND_CLUSTER:-agent-workspace}"
context="kind-${cluster}"
controller_image="${CONTROLLER_IMAGE:-agent-workspace:local}"
token="${AGENT_WORKSPACE_TOKEN:-$(openssl rand -hex 16)}"
results="${RESULTS_FILE:-}"
k="kubectl --context $context -n $namespace"
api=http://127.0.0.1:8090
llm=http://127.0.0.1:8081
ws=c1
body="$(mktemp)"

say() { printf '%s\n' "$*"; [ -z "$results" ] || printf '%s\n' "$*" >> "$results"; }
ctl() { curl -fsS -H "X-Control-Token: $token" "$@"; }
fail() { echo "FAIL: $*" >&2; ctl "$api/v1/workspaces/$ws" 2>/dev/null | jq -c . >&2 || true; exit 1; }
now() { python3 -c 'import time;print(int(time.time()))'; }

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" >/dev/null 2>&1 || true; done
  rm -f "$body"
  kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl --context "$context" create namespace "$namespace" >/dev/null
$k create secret generic agent-workspace-auth --from-literal=token="$token" >/dev/null
$k create configmap agent-workspace-profiles --from-file=profiles.json=configs/profiles.json >/dev/null
# Short timers so the idle step takes under a minute; the defaults are tuned
# for production.
sed 's|"-idle", "2m"|"-idle", "40s", "-upgrade-settle", "5s", "-heartbeat-interval", "3s", "-reconcile", "2s"|' \
  deploy/controller.yaml | kubectl --context "$context" apply -f - >/dev/null
$k set image deployment/agent-workspace-controller controller="$controller_image" >/dev/null
kubectl --context "$context" apply -f deploy/fake-llm.yaml >/dev/null
$k rollout status deployment/agent-workspace-controller --timeout=120s >/dev/null
$k rollout status deployment/fake-llm --timeout=120s >/dev/null

$k port-forward svc/agent-workspace-controller 8090:8090 >/tmp/kind-continuity-ctl.log 2>&1 &
pids+=($!)
$k port-forward svc/fake-llm 8081:8081 >/tmp/kind-continuity-llm.log 2>&1 &
pids+=($!)
for _ in $(seq 1 30); do curl -fsS $api/health >/dev/null 2>&1 && curl -fsS $llm/health >/dev/null 2>&1 && break; sleep 1; done

# A request through the gateway wakes a stopped workspace, so a generous cap.
chat() { # message -> http code; body in $body
  curl -sS -m 150 -o "$body" -w '%{http_code}' -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
    -d "{\"session\":\"cont\",\"message\":\"$1\"}" "$api/w/$ws/v1/chat" || true
}
chat_ok() { [ "$(chat "$1")" = 200 ]; }
w() { ctl "$api/v1/workspaces/$ws" | jq -r "$1"; }
wait_for() { # description timeout command...
  local what="$1" limit="$2"; shift 2
  local deadline=$(( $(now) + limit ))
  until "$@" >/dev/null 2>&1; do
    [ "$(now)" -lt "$deadline" ] || fail "timed out after ${limit}s waiting for: $what"
    sleep 1
  done
}
phase_is() { [ "$(w .phase)" = "$1" ]; }
no_pod() { [ -z "$($k get pods -l agent-workspace/workspace=$ws -o name)" ]; }
pod_uid() { $k get pod -l agent-workspace/workspace=$ws -o jsonpath='{.items[0].metadata.uid}'; }
pvc_uid() { $k get pvc "nc-$ws" -o jsonpath='{.metadata.uid}'; }
outcome_is() { [ "$(w '.last_upgrade.outcome // ""')" = "$1" ]; }
version_is() { [ "$(w '.agent_version // ""')" = "$1" ]; }
running_ok() { phase_is running && chat_ok "ping"; }

plant() { # prints the token
  local t="zx-$(openssl rand -hex 6)"
  chat_ok "remember $t" || fail "could not plant $t: $(cat "$body")"
  printf '%s' "$t"
}
expect_recall() { # token
  chat_ok "recall $1" || fail "recall request failed: $(cat "$body")"
  [ "$(jq -r .reply "$body")" = "recall(found): $1" ] || fail "the model was not sent $1: $(jq -r .reply "$body")"
}

ctl -H 'Content-Type: application/json' -d "{\"id\":\"$ws\",\"profile\":\"agent\"}" $api/v1/workspaces >/dev/null
ctl -X PUT -H 'Content-Type: application/json' -d '{"values":{"llm_api_key":"key-v1"}}' $api/v1/workspaces/$ws/credentials >/dev/null
ctl -X POST -H 'X-Biz-Id: cont-start-1' $api/v1/workspaces/$ws/start >/dev/null
wait_for "agent to answer" 180 chat_ok "hello"
volume="$(pvc_uid)"

say "## 1. explicit stop, then wake by a gateway request"
t1="$(plant)"
uid_before="$(pod_uid)"
ctl -X POST -H 'X-Biz-Id: cont-stop-1' $api/v1/workspaces/$ws/stop >/dev/null
wait_for "workspace to stop" 120 phase_is stopped
wait_for "pod to go away" 120 no_pod
expect_recall "$t1"
[ "$(pod_uid)" != "$uid_before" ] || fail "the same pod answered, so nothing was woken"
[ "$(pvc_uid)" = "$volume" ] || fail "the volume was replaced"
say "- stopped, no pod; a request started pod $(pod_uid) (was $uid_before), same PVC; the LLM was sent the token"

say "## 2. idle scale-to-zero, then wake by a request"
t2="$(plant)"
uid_before="$(pod_uid)"
wait_for "idle reaper to stop the workspace" 240 phase_is stopped
wait_for "pod to go away" 120 no_pod
ctl "$api/v1/audit?workspace=$ws&action=idle-stop&limit=10" | jq -e 'length >= 1' >/dev/null || fail "the stop was not the idle reaper"
expect_recall "$t2"
[ "$(pod_uid)" != "$uid_before" ] || fail "the same pod answered"
say "- idle-stop in the audit log, no pod, then a request started pod $(pod_uid); the LLM was sent the token"

say "## 3. image upgrade, committed"
t3="$(plant)"
uid_before="$(pod_uid)"
ctl -X POST -H 'Content-Type: application/json' -d '{"image":"agent-workspace-agent:v2","timeout_seconds":180}' $api/v1/workspaces/$ws/upgrade >/dev/null
wait_for "upgrade to commit" 240 outcome_is committed
wait_for "heartbeat to report v2" 60 version_is v2
wait_for "workspace running" 120 running_ok
expect_recall "$t3"
[ "$(pod_uid)" != "$uid_before" ] || fail "the upgrade did not replace the pod"
say "- committed, agent_version=v2, new pod, the LLM was sent the token"

say "## 4. image upgrade that crashes on start, rolled back"
t4="$(plant)"
ctl -X POST -H 'Content-Type: application/json' -d '{"image":"agent-workspace-agent:bad","timeout_seconds":30}' $api/v1/workspaces/$ws/upgrade >/dev/null
wait_for "automatic rollback" 180 outcome_is rolled-back
wait_for "workspace running again" 180 running_ok
version_is v2 || wait_for "heartbeat to report v2" 60 version_is v2
expect_recall "$t4"
[ "$(pvc_uid)" = "$volume" ] || fail "the volume was replaced"
say "- rolled back ($(w .last_upgrade.reason)), serving again, the LLM was sent the token"

say "## 5. the token count survives pod replacement and matches the provider"
billed() { curl -fsS $llm/admin/stats | jq '.prompt_tokens + .completion_tokens'; }
counts_match() { [ "$(w .usage.total_tokens)" = "$(billed)" ]; }
wait_for "controller usage to equal what the LLM endpoint billed" 60 counts_match
say "- usage.total_tokens=$(w .usage.total_tokens), billed by the LLM endpoint=$(billed)"

say "## 6. token budget: suspend, refuse with 402, raise, resume"
used="$(w .usage.total_tokens)"
ctl -X PUT -H 'Content-Type: application/json' -d "{\"token_budget\":$(( used + 50 ))}" $api/v1/workspaces/$ws/token-budget >/dev/null
for i in 1 2 3 4 5 6; do
  [ "$(w .desired)" = suspended ] && break
  chat_ok "burn $i $(openssl rand -hex 8)" || true
  sleep 4
done
wait_for "budget suspension" 60 phase_is suspended
[ "$(w .suspended_for)" = token_budget ] || fail "suspended for $(w .suspended_for)"
no_pod || wait_for "pod to go away" 120 no_pod
code="$(chat "are you there")"
[ "$code" = 402 ] || fail "gateway answered $code for a workspace over budget: $(cat "$body")"
ctl "$api/v1/audit?workspace=$ws&action=budget-exceeded&limit=10" | jq -e 'length == 1' >/dev/null || fail "expected exactly one budget-exceeded event"
say "- suspended_for=token_budget, last_error='$(w .last_error)', gateway answered 402"
ctl -X PUT -H 'Content-Type: application/json' -d '{"token_budget":0}' $api/v1/workspaces/$ws/token-budget >/dev/null
wait_for "workspace to answer after the budget was cleared" 180 chat_ok "back again"
say "- budget cleared: workspace woke on the next request"

say "## audit"
ctl "$api/v1/audit?workspace=$ws&limit=300" | jq -r '.[] | select(.action|test("idle|upgrade|budget|resume|stop|usage")) | "\(.action) \(.result) \(.detail)"' | while read -r line; do say "- $line"; done
say "continuity e2e passed"
