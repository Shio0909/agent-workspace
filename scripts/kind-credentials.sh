#!/usr/bin/env bash
# End-to-end check of credential rotation on a kind cluster.
#
# It proves four things with the real controller, a real agent pod and a fake
# LLM endpoint:
#   1. a key set before start reaches the pod and the agent can use it;
#   2. after the provider revokes the old key, the agent is refused;
#   3. PUT /credentials delivers the new key to the SAME pod (same UID, same
#      process, zero restarts) and the agent recovers; the time that takes is
#      printed, because the kubelet refreshes mounted Secrets on its own sync
#      period rather than instantly;
#   4. stop/start keeps the key and the conversation, and a hard delete removes
#      the Secret.
#
# Images: build with `docker build --target {controller,agent,fakellm}` and load
# them into the cluster first (see the usage below).
set -euo pipefail

namespace="${NAMESPACE:-agent-workspace}"
cluster="${KIND_CLUSTER:-agent-workspace}"
context="kind-${cluster}"
controller_image="${CONTROLLER_IMAGE:-agent-workspace:local}"
rotate_timeout="${ROTATE_TIMEOUT:-180}"
token="${AGENT_WORKSPACE_TOKEN:-$(openssl rand -hex 16)}"
results="${RESULTS_FILE:-}"
k="kubectl --context $context -n $namespace"
api=http://127.0.0.1:8090
llm=http://127.0.0.1:8081

say() { printf '%s\n' "$*"; [ -z "$results" ] || printf '%s\n' "$*" >> "$results"; }
ctl() { curl -fsS -H "X-Control-Token: $token" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" >/dev/null 2>&1 || true; done
  kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl --context "$context" create namespace "$namespace" >/dev/null
$k create secret generic agent-workspace-auth --from-literal=token="$token" >/dev/null
$k create configmap agent-workspace-profiles --from-file=profiles.json=configs/profiles.json >/dev/null
kubectl --context "$context" apply -f deploy/controller.yaml >/dev/null
$k set image deployment/agent-workspace-controller controller="$controller_image" >/dev/null
kubectl --context "$context" apply -f deploy/fake-llm.yaml >/dev/null
$k rollout status deployment/agent-workspace-controller --timeout=120s >/dev/null
$k rollout status deployment/fake-llm --timeout=120s >/dev/null

$k port-forward svc/agent-workspace-controller 8090:8090 >/tmp/kind-credentials-ctl.log 2>&1 &
pids+=($!)
$k port-forward svc/fake-llm 8081:8081 >/tmp/kind-credentials-llm.log 2>&1 &
pids+=($!)
for _ in $(seq 1 30); do curl -fsS $api/health >/dev/null 2>&1 && curl -fsS $llm/health >/dev/null 2>&1 && break; sleep 1; done

chat() { # session message -> prints "<http-code> <body>"
  curl -sS -o /tmp/kind-credentials-body -w '%{http_code}' -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
    -d "{\"session\":\"$1\",\"message\":\"$2\"}" "$api/w/a1/v1/chat" || true
}
jqb() { jq -r "$1" /tmp/kind-credentials-body; }

ctl -H 'Content-Type: application/json' -d '{"id":"a1","profile":"agent"}' $api/v1/workspaces >/dev/null

say "## 1. key set before start"
ctl -X PUT -H 'Content-Type: application/json' -d '{"values":{"llm_api_key":"key-v1"}}' $api/v1/workspaces/a1/credentials >/dev/null
ctl -X POST -H 'X-Biz-Id: cred-start-1' $api/v1/workspaces/a1/start >/dev/null
for _ in $(seq 1 60); do
  [ "$(chat s1 hello)" = 200 ] && break
  sleep 2
done
[ "$(chat s1 hello2)" = 200 ] || fail "agent did not answer with the initial key: $(cat /tmp/kind-credentials-body)"
[ "$(jqb .credential_version)" = 1 ] || fail "expected credential_version 1"
pod_uid="$($k get pod -l agent-workspace/workspace=a1 -o jsonpath='{.items[0].metadata.uid}')"
pid_before="$(jqb .pid)"
say "- agent answered with credential_version=1 (pod $pod_uid, pid $pid_before)"

say "## 2. provider revokes key-v1"
curl -fsS -X PUT -d '{"keys":["key-v2"]}' $llm/admin/keys >/dev/null
[ "$(chat s1 revoked)" = 502 ] || fail "revoked key was not refused: $(cat /tmp/kind-credentials-body)"
say "- agent refused with 502, still on credential_version=$(jqb .credential_version)"

say "## 3. rotate to key-v2 without restarting the pod"
start_ns=$(python3 -c 'import time;print(time.time_ns())')
ctl -X PUT -H 'Content-Type: application/json' -d '{"values":{"llm_api_key":"key-v2"}}' $api/v1/workspaces/a1/credentials >/dev/null
deadline=$(( $(date +%s) + rotate_timeout ))
until [ "$(chat s1 after-rotation)" = 200 ]; do
  [ "$(date +%s)" -lt "$deadline" ] || fail "agent did not pick up key-v2 within ${rotate_timeout}s"
  sleep 0.5
done
elapsed_ms=$(( ( $(python3 -c 'import time;print(time.time_ns())') - start_ns ) / 1000000 ))
[ "$(jqb .credential_version)" = 2 ] || fail "expected credential_version 2"
pod_uid_after="$($k get pod -l agent-workspace/workspace=a1 -o jsonpath='{.items[0].metadata.uid}')"
restarts="$($k get pod -l agent-workspace/workspace=a1 -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')"
pid_after="$(jqb .pid)"
[ "$pod_uid" = "$pod_uid_after" ] || fail "pod was replaced during rotation"
[ "$pid_before" = "$pid_after" ] || fail "agent process restarted during rotation"
[ "$restarts" = 0 ] || fail "container restarted during rotation"
say "- recovered on credential_version=2 after ${elapsed_ms} ms; same pod, same pid ($pid_after), restartCount=$restarts"
say "- ROTATION_LATENCY_MS=$elapsed_ms"

say "## 4. stop/start keeps key and conversation; hard delete removes the Secret"
ctl -X POST -H 'X-Biz-Id: cred-stop-1' $api/v1/workspaces/a1/stop >/dev/null
for _ in $(seq 1 60); do
  [ "$(ctl $api/v1/workspaces/a1 | jq -r .phase)" = stopped ] && break
  sleep 1
done
$k get secret nc-a1-cred >/dev/null || fail "stop removed the credential secret"
for _ in $(seq 1 60); do
  [ "$(chat s1 resumed)" = 200 ] && break
  sleep 2
done
[ "$(jqb .credential_version)" = 2 ] || fail "key was lost across stop/start: $(cat /tmp/kind-credentials-body)"
case "$(jqb .reply)" in
  echo\(4\)*) ;;
  *) fail "conversation was lost across stop/start: $(jqb .reply)" ;;
esac
say "- after stop/start: credential_version=2, history replayed from the volume ($(jqb .reply))"

ctl -X POST -H 'X-Biz-Id: cred-stop-2' $api/v1/workspaces/a1/stop >/dev/null || true
ctl -X DELETE -H 'X-Biz-Id: cred-delete-1' $api/v1/workspaces/a1 >/dev/null
for _ in $(seq 1 60); do
  $k get secret nc-a1-cred >/dev/null 2>&1 || { gone=1; break; }
  sleep 1
done
[ "${gone:-0}" = 1 ] || fail "hard delete left the credential secret behind"
say "- hard delete removed the credential secret"

say "## audit (values never appear)"
ctl "$api/v1/audit?workspace=a1&limit=100" | jq -r '.[] | select(.action|startswith("credential")) | "\(.action) \(.result) \(.detail)"' | while read -r line; do say "- $line"; done
if ctl "$api/v1/audit?workspace=a1&limit=100" | grep -q 'key-v'; then fail "a credential value appears in the audit log"; fi
say "credential rotation e2e passed"
