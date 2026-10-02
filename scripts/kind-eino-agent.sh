#!/usr/bin/env bash
# End-to-end check of a real, third-party agent (eino_agent, a Go RAG service
# with its own PostgreSQL) running as an agent-workspace profile against a
# real OpenAI-compatible LLM endpoint. There is no fake model here: a random
# token is planted in one conversation, the workspace is disturbed, and the
# model has to repeat the token from the session history that eino_agent keeps
# in PostgreSQL on the workspace volume.
#
#   LLM_BASE_URL   endpoint base, e.g. https://example.com/v1
#   LLM_MODEL      model id
#   LLM_KEY_FILE   file holding the API key. It is only ever piped into the
#                  credentials API, never put on a command line or printed.
#
# Images that must be loaded into the cluster first (make e2e-eino-agent does
# this): agent-workspace:local, eino-agent-workspace:{local,v2,bad}.
#
#   1. explicit stop, then wake by a gateway request
#   2. idle scale-to-zero, then wake by a request
#   3. image upgrade, committed (the pod and its PostgreSQL are replaced)
#   4. image upgrade that never starts, rolled back
#   5. credential rotation: a wrong key breaks model calls, the right key
#      replaces the pod (the profile cannot reload a key at runtime) and the
#      session is still there
#   6. the controller's token count equals what the pod itself reports, after
#      the pod was replaced four times, and no regression was audited
#   7. a token budget suspends the workspace, the gateway answers 402, and
#      clearing the budget brings it back
set -euo pipefail

: "${LLM_BASE_URL:?set LLM_BASE_URL}"
: "${LLM_MODEL:?set LLM_MODEL}"
: "${LLM_KEY_FILE:?set LLM_KEY_FILE}"

namespace="${NAMESPACE:-agent-workspace}"
cluster="${KIND_CLUSTER:-agent-workspace}"
context="kind-${cluster}"
controller_image="${CONTROLLER_IMAGE:-agent-workspace:local}"
token="${AGENT_WORKSPACE_TOKEN:-$(openssl rand -hex 16)}"
results="${RESULTS_FILE:-}"
k="kubectl --context $context -n $namespace"
api=http://127.0.0.1:8090
ws=e1
sid=""
body="$(mktemp)"
profiles="$(mktemp)"

say() { printf '%s\n' "$*"; [ -z "$results" ] || printf '%s\n' "$*" >> "$results"; }
ctl() { curl -fsS -H "X-Control-Token: $token" "$@"; }
fail() { echo "FAIL: $*" >&2; ctl "$api/v1/workspaces/$ws" 2>/dev/null | jq -c . >&2 || true; exit 1; }
now() { python3 -c 'import time;print(int(time.time()))'; }

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" >/dev/null 2>&1 || true; done
  rm -f "$body" "$profiles"
  kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Same profile as configs/profiles.json, pointed at the real endpoint. Neither
# value is secret.
jq --arg u "$LLM_BASE_URL" --arg m "$LLM_MODEL" \
  '.["eino-agent"].env.LLM_BASE_URL=$u | .["eino-agent"].env.LLM_MODEL=$m' configs/profiles.json > "$profiles"

kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl --context "$context" create namespace "$namespace" >/dev/null
$k create secret generic agent-workspace-auth --from-literal=token="$token" >/dev/null
$k create configmap agent-workspace-profiles --from-file=profiles.json="$profiles" >/dev/null
sed 's|"-idle", "2m"|"-idle", "60s", "-upgrade-settle", "5s", "-heartbeat-interval", "3s", "-reconcile", "2s"|' \
  deploy/controller.yaml | kubectl --context "$context" apply -f - >/dev/null
$k set image deployment/agent-workspace-controller controller="$controller_image" >/dev/null
$k rollout status deployment/agent-workspace-controller --timeout=120s >/dev/null

$k port-forward svc/agent-workspace-controller 8090:8090 >/tmp/kind-eino-ctl.log 2>&1 &
pids+=($!)
for _ in $(seq 1 30); do curl -fsS $api/health >/dev/null 2>&1 && break; sleep 1; done

# A request through the gateway wakes a stopped workspace, so a generous cap.
# The first call has no session; eino_agent creates one and returns its id.
chat() { # message -> http code; body in $body
  local session=""
  [ -z "$sid" ] || session=",\"session_id\":\"$sid\""
  curl -sS -m 170 -o "$body" -w '%{http_code}' -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
    -d "{\"query\":\"$1\",\"use_agent\":true$session}" "$api/w/$ws/api/v1/chat" || true
}
chat_ok() { [ "$(chat "$1")" = 200 ]; }
w() { ctl "$api/v1/workspaces/$ws" | jq -r "$1"; }
pod_heartbeat() { curl -fsS -H "X-Control-Token: $token" "$api/w/$ws/heartbeat"; }
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
put_key() { # file
  jq -Rs '{values:{llm_api_key:rtrimstr("\n")}}' "$1" | ctl -X PUT -H 'Content-Type: application/json' -d @- "$api/v1/workspaces/$ws/credentials" >/dev/null
}

plant() { # prints the token
  local t="zx-$(openssl rand -hex 6)"
  chat_ok "请记住暗号 $t,以后我会问你。只回复已记住。" || fail "could not plant $t: $(cat "$body")"
  [ -n "$sid" ] || sid="$(jq -r .session_id "$body")"
  printf '%s' "$t"
}
expect_recall() { # token
  chat_ok "我之前让你记的暗号是什么?只回复暗号本身。" || fail "recall request failed: $(cat "$body")"
  [ "$(jq -r .answer "$body")" = "$1" ] || fail "expected $1, the model answered: $(jq -r .answer "$body")"
}

ctl -H 'Content-Type: application/json' -d "{\"id\":\"$ws\",\"profile\":\"eino-agent\"}" $api/v1/workspaces >/dev/null
put_key "$LLM_KEY_FILE"
ctl -X POST -H 'X-Biz-Id: eino-start-1' $api/v1/workspaces/$ws/start >/dev/null
# First start: initdb, migrations 1-13 and the server, all inside the pod.
wait_for "eino_agent to answer" 300 chat_ok "你好"
volume="$(pvc_uid)"
sid="$(jq -r .session_id "$body")"

say "## 1. explicit stop, then wake by a gateway request"
t1="$(plant)"
uid_before="$(pod_uid)"
ctl -X POST -H 'X-Biz-Id: eino-stop-1' $api/v1/workspaces/$ws/stop >/dev/null
wait_for "workspace to stop" 120 phase_is stopped
wait_for "pod to go away" 120 no_pod
expect_recall "$t1"
[ "$(pod_uid)" != "$uid_before" ] || fail "the same pod answered, so nothing was woken"
[ "$(pvc_uid)" = "$volume" ] || fail "the volume was replaced"
say "- stopped, no pod; a request started pod $(pod_uid) (was $uid_before), same PVC; the real model repeated the token"

say "## 2. idle scale-to-zero, then wake by a request"
t2="$(plant)"
uid_before="$(pod_uid)"
wait_for "idle reaper to stop the workspace" 300 phase_is stopped
wait_for "pod to go away" 120 no_pod
ctl "$api/v1/audit?workspace=$ws&action=idle-stop&limit=10" | jq -e 'length >= 1' >/dev/null || fail "the stop was not the idle reaper"
expect_recall "$t2"
[ "$(pod_uid)" != "$uid_before" ] || fail "the same pod answered"
say "- idle-stop in the audit log, no pod, then a request started pod $(pod_uid); the real model repeated the token"

say "## 3. image upgrade, committed"
t3="$(plant)"
uid_before="$(pod_uid)"
ctl -X POST -H 'Content-Type: application/json' -d '{"image":"eino-agent-workspace:v2","timeout_seconds":240}' $api/v1/workspaces/$ws/upgrade >/dev/null
wait_for "upgrade to commit" 300 outcome_is committed
wait_for "heartbeat to report v2" 60 version_is v2
wait_for "workspace running" 120 running_ok
expect_recall "$t3"
[ "$(pod_uid)" != "$uid_before" ] || fail "the upgrade did not replace the pod"
say "- committed, agent_version=v2 from /heartbeat, new pod, the real model repeated the token"

say "## 4. image upgrade that never starts, rolled back"
t4="$(plant)"
ctl -X POST -H 'Content-Type: application/json' -d '{"image":"eino-agent-workspace:bad","timeout_seconds":40}' $api/v1/workspaces/$ws/upgrade >/dev/null
wait_for "automatic rollback" 240 outcome_is rolled-back
wait_for "workspace running again" 240 running_ok
version_is v2 || wait_for "heartbeat to report v2" 60 version_is v2
expect_recall "$t4"
[ "$(pvc_uid)" = "$volume" ] || fail "the volume was replaced"
say "- rolled back ($(w .last_upgrade.reason)), serving again, the real model repeated the token"

say "## 5. credential rotation"
t5="$(plant)"
uid_before="$(pod_uid)"
bad="$(mktemp)"; printf 'sk-not-a-real-key' > "$bad"
put_key "$bad"; rm -f "$bad"
replaced() { local u; u="$(pod_uid 2>/dev/null || true)"; [ -n "$u" ] && [ "$u" != "$uid_before" ]; }
wait_for "workload replacement after the credential change" 240 replaced
wait_for "replacement pod ready" 240 phase_is running
code=""; for _ in $(seq 1 60); do code="$(chat 'ping')"; [ "$code" != 502 ] && [ "$code" != 503 ] && [ "$code" != 504 ] && break; sleep 2; done
wrong_key_reply="$code $(jq -r '.answer // .error.message // .error // .' "$body" 2>/dev/null | head -c 120)"
put_key "$LLM_KEY_FILE"
wait_for "right key to work again" 300 chat_ok "ping"
expect_recall "$t5"
ctl "$api/v1/audit?workspace=$ws&action=restart&limit=20" | jq -e 'map(select(.detail|test("credentials"))) | length >= 2' >/dev/null || fail "credential changes did not replace the workload twice"
say "- wrong key: $wrong_key_reply; both changes replaced the pod; with the right key the session was intact and the real model repeated the token"

say "## 6. the token count survives pod replacement and matches the pod"
counts_match() { [ "$(w .usage.total_tokens)" = "$(pod_heartbeat | jq .usage.total_tokens)" ]; }
wait_for "controller usage to equal the pod's own count" 60 counts_match
[ "$(w .usage.total_tokens)" -gt 0 ] || fail "no usage was recorded"
ctl "$api/v1/audit?workspace=$ws&action=usage-regressed&limit=10" | jq -e 'length == 0' >/dev/null || fail "a usage regression was audited although the volume was never lost"
say "- controller usage.total_tokens=$(w .usage.total_tokens), pod /heartbeat total_tokens=$(pod_heartbeat | jq .usage.total_tokens), no usage-regressed audit"

say "## 7. token budget: suspend, refuse with 402, clear, resume"
used="$(w .usage.total_tokens)"
ctl -X PUT -H 'Content-Type: application/json' -d "{\"token_budget\":$(( used + 50 ))}" $api/v1/workspaces/$ws/token-budget >/dev/null
for i in 1 2 3 4 5 6; do
  [ "$(w .desired)" = suspended ] && break
  chat_ok "说一个 1 到 100 之间的数字 $i" || true
  sleep 4
done
wait_for "budget suspension" 90 phase_is suspended
[ "$(w .suspended_for)" = token_budget ] || fail "suspended for $(w .suspended_for)"
no_pod || wait_for "pod to go away" 120 no_pod
code="$(chat "are you there")"
[ "$code" = 402 ] || fail "gateway answered $code for a workspace over budget: $(cat "$body")"
ctl "$api/v1/audit?workspace=$ws&action=budget-exceeded&limit=10" | jq -e 'length == 1' >/dev/null || fail "expected exactly one budget-exceeded event"
say "- suspended_for=token_budget at usage $(w .usage.total_tokens) (budget $(( used + 50 ))), gateway answered 402"
ctl -X PUT -H 'Content-Type: application/json' -d '{"token_budget":0}' $api/v1/workspaces/$ws/token-budget >/dev/null
wait_for "workspace to answer after the budget was cleared" 300 chat_ok "ping"
expect_recall "$t5"
say "- budget cleared: workspace woke on the next request and the real model still repeated the earlier token"

say "## audit"
ctl "$api/v1/audit?workspace=$ws&limit=300" | jq -r '.[] | select(.action|test("idle|upgrade|budget|resume|stop|usage|restart|credential")) | "\(.action) \(.result) \(.detail)"' | while read -r line; do say "- $line"; done
say "eino-agent e2e passed"
