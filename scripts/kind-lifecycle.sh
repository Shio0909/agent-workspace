#!/usr/bin/env bash
# End-to-end check of image upgrade, automatic rollback and heartbeat recovery
# on a kind cluster, with the real controller and real agent pods.
#
#   1. a workspace reports its agent version through the heartbeat;
#   2. an upgrade to a good image is committed, and the conversation, the
#      credential and the volume survive it;
#   3. an upgrade to an image that crashes on start is rolled back by itself and
#      the workspace keeps serving on the previous image;
#   4. an upgrade to an image which cannot be pulled is rolled back as well;
#   5. a manual rollback returns to the profile's own image;
#   6. a workload which is stopped (not crashed) is detected through the missing
#      heartbeat and restarted, and its conversation is still there.
#
# Images that must be loaded into the cluster first:
#   agent-workspace:local             docker build --target controller
#   agent-workspace-agent:local       docker build --target agent --build-arg AGENT_VERSION=v1
#   agent-workspace-agent:v2          the same with AGENT_VERSION=v2
#   agent-workspace-agent:bad         FROM agent-workspace-agent:v2 + ENTRYPOINT ["false"]
#   agent-workspace-fakellm:local     docker build --target fakellm
# "agent-workspace-agent:missing" is deliberately never loaded.
set -euo pipefail

namespace="${NAMESPACE:-agent-workspace}"
cluster="${KIND_CLUSTER:-agent-workspace}"
context="kind-${cluster}"
controller_image="${CONTROLLER_IMAGE:-agent-workspace:local}"
token="${AGENT_WORKSPACE_TOKEN:-$(openssl rand -hex 16)}"
results="${RESULTS_FILE:-}"
k="kubectl --context $context -n $namespace"
api=http://127.0.0.1:8090
body="$(mktemp)"

say() { printf '%s\n' "$*"; [ -z "$results" ] || printf '%s\n' "$*" >> "$results"; }
ctl() { curl -fsS -H "X-Control-Token: $token" "$@"; }
fail() { echo "FAIL: $*" >&2; ctl "$api/v1/workspaces/a1" 2>/dev/null | jq -c . >&2 || true; exit 1; }
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
# Short timers so the scenarios finish in minutes; the defaults are tuned for
# production, where nothing should react this fast.
sed 's|"-idle", "2m"|"-idle", "30m", "-upgrade-settle", "5s", "-heartbeat-interval", "3s", "-heartbeat-misses", "3", "-restart-cooldown", "30s", "-reconcile", "2s"|' \
  deploy/controller.yaml | kubectl --context "$context" apply -f - >/dev/null
$k set image deployment/agent-workspace-controller controller="$controller_image" >/dev/null
kubectl --context "$context" apply -f deploy/fake-llm.yaml >/dev/null
$k rollout status deployment/agent-workspace-controller --timeout=120s >/dev/null
$k rollout status deployment/fake-llm --timeout=120s >/dev/null
$k port-forward svc/agent-workspace-controller 8090:8090 >/tmp/kind-lifecycle-ctl.log 2>&1 &
pids+=($!)
for _ in $(seq 1 30); do curl -fsS $api/health >/dev/null 2>&1 && break; sleep 1; done

chat() { # session message -> http code; body in $body
  curl -sS -m 60 -o "$body" -w '%{http_code}' -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
    -d "{\"session\":\"$1\",\"message\":\"$2\"}" "$api/w/a1/v1/chat" || true
}
ws() { ctl "$api/v1/workspaces/a1" | jq -r "$1"; }
wait_for() { # description timeout command...
  local what="$1" limit="$2"; shift 2
  local deadline=$(( $(now) + limit ))
  until "$@" >/dev/null 2>&1; do
    [ "$(now)" -lt "$deadline" ] || fail "timed out after ${limit}s waiting for: $what"
    sleep 1
  done
}
chat_ok() { [ "$(chat "$1" "$2")" = 200 ]; }
deploy_image() { $k get deployment nc-a1 -o jsonpath='{.spec.template.spec.containers[0].image}'; }
outcome_is() { [ "$(ws '.last_upgrade.outcome // ""')" = "$1" ]; }
version_is() { [ "$(ws '.agent_version // ""')" = "$1" ]; }
running_ok() { [ "$(ws .phase)" = running ] && chat_ok probe ping; }
image_is() { [ "$(deploy_image)" = "$1" ]; }
upgrade_ended() { # target outcome
  local o; o="$(ctl "$api/v1/workspaces/a1")"
  [ "$(jq -r '.last_upgrade.to // ""' <<<"$o")" = "$1" ] && [ "$(jq -r '.last_upgrade.outcome // ""' <<<"$o")" = "$2" ]
}
flag_cleared() { [ "$(ws '.unresponsive // false')" = false ]; }
pull_failing() {
  $k get pods -l agent-workspace/workspace=a1 -o jsonpath='{.items[*].status.containerStatuses[*].state.waiting.reason}' | grep -Eq 'ErrImagePull|ImagePullBackOff'
}

ctl -H 'Content-Type: application/json' -d '{"id":"a1","profile":"agent"}' $api/v1/workspaces >/dev/null
ctl -X PUT -H 'Content-Type: application/json' -d '{"values":{"llm_api_key":"key-v1"}}' $api/v1/workspaces/a1/credentials >/dev/null
$k port-forward svc/fake-llm 8081:8081 >/tmp/kind-lifecycle-llm.log 2>&1 &
pids+=($!)
sleep 2
curl -fsS -X PUT -d '{"keys":["key-v1"]}' http://127.0.0.1:8081/admin/keys >/dev/null

say "## 1. start, heartbeat reports the agent version"
ctl -X POST -H 'X-Biz-Id: life-start-1' $api/v1/workspaces/a1/start >/dev/null
wait_for "agent to answer" 180 chat_ok conv "first message"
chat_ok conv "second message" || fail "chat failed"
wait_for "heartbeat to report v1" 60 version_is v1
say "- agent_version=$(ws .agent_version), heartbeat_at=$(ws .heartbeat_at)"
[ "$(deploy_image)" = agent-workspace-agent:local ] || fail "unexpected starting image $(deploy_image)"
volume_before="$($k get pvc nc-a1 -o jsonpath='{.metadata.uid}')"

say "## 2. upgrade to a good image"
t0=$(now)
ctl -X POST -H 'Content-Type: application/json' -d '{"image":"agent-workspace-agent:v2","timeout_seconds":180}' $api/v1/workspaces/a1/upgrade >/dev/null
say "- upgrade accepted: $(ws '.upgrade.from') -> $(ws '.upgrade.to')"
wait_for "upgrade to commit" 240 outcome_is committed
say "- committed after $(( $(now) - t0 )) s (UPGRADE_COMMIT_S=$(( $(now) - t0 )))"
wait_for "heartbeat to report v2" 60 version_is v2
[ "$(deploy_image)" = agent-workspace-agent:v2 ] || fail "deployment image is $(deploy_image)"
wait_for "chat after upgrade" 90 chat_ok conv "after upgrade"
case "$(jq -r .reply "$body")" in
  echo\(3\)*) ;;
  *) fail "conversation did not survive the upgrade: $(jq -r .reply "$body")" ;;
esac
[ "$(jq -r .credential_version "$body")" = 1 ] || fail "credential lost in upgrade"
[ "$($k get pvc nc-a1 -o jsonpath='{.metadata.uid}')" = "$volume_before" ] || fail "volume was replaced"
say "- agent_version=v2, history replayed from the volume ($(jq -r .reply "$body")), same PVC, credential_version=1"
[ "$(ws .previous_image)" = agent-workspace-agent:local ] || fail "previous_image not recorded: $(ws .previous_image)"

say "## 3. upgrade to an image that crashes on start"
t0=$(now)
ctl -X POST -H 'Content-Type: application/json' -d '{"image":"agent-workspace-agent:bad","timeout_seconds":40}' $api/v1/workspaces/a1/upgrade >/dev/null
wait_for "automatic rollback" 180 outcome_is rolled-back
say "- rolled back after $(( $(now) - t0 )) s: $(ws .last_upgrade.reason) (ROLLBACK_S=$(( $(now) - t0 )))"
wait_for "workspace running again" 180 running_ok
[ "$(deploy_image)" = agent-workspace-agent:v2 ] || fail "rollback left image $(deploy_image)"
version_is v2 || wait_for "heartbeat v2 after rollback" 60 version_is v2
chat_ok conv "after rollback" || fail "no answer after rollback"
say "- serving again on $(deploy_image); reply: $(jq -r .reply "$body")"

say "## 4. upgrade to an image that cannot be pulled"
ctl -X POST -H 'Content-Type: application/json' -d '{"image":"agent-workspace-agent:missing","timeout_seconds":30}' $api/v1/workspaces/a1/upgrade >/dev/null
saw_pull_error=no
for _ in $(seq 1 25); do pull_failing && { saw_pull_error=yes; break; }; sleep 1; done
wait_for "automatic rollback" 180 upgrade_ended agent-workspace-agent:missing rolled-back
wait_for "workspace running again" 180 running_ok
[ "$(deploy_image)" = agent-workspace-agent:v2 ] || fail "rollback left image $(deploy_image)"
say "- unpullable image rolled back (pull error observed: $saw_pull_error); serving on $(deploy_image)"

say "## 5. manual rollback to the profile image"
ctl -X POST $api/v1/workspaces/a1/rollback >/dev/null
wait_for "image to change" 180 image_is agent-workspace-agent:local
wait_for "workspace running on v1" 180 running_ok
wait_for "heartbeat to report v1" 60 version_is v1
say "- back on $(deploy_image), agent_version=$(ws .agent_version)"

say "## 6. a stopped (not crashed) workload is restarted through the missing heartbeat"
node="$($k get pod -l agent-workspace/workspace=a1 -o jsonpath='{.items[0].spec.nodeName}')"
uid_before="$($k get pod -l agent-workspace/workspace=a1 -o jsonpath='{.items[0].metadata.uid}')"
docker exec "$node" pkill -STOP -x agent-runtime || fail "could not freeze the agent process on $node"
t0=$(now)
lost() { ctl "$api/v1/audit?workspace=a1&limit=200" | jq -e '[.[]|select(.action=="heartbeat-restart")]|length>=1'; }
wait_for "controller to notice and restart" 180 lost
say "- restart requested after $(( $(now) - t0 )) s of silence (DETECT_S=$(( $(now) - t0 )))"
new_pod() { [ "$($k get pod -l agent-workspace/workspace=a1 -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.uid}' | tr ' ' '\n' | grep -vc "^$uid_before$")" -ge 1 ]; }
wait_for "replacement pod" 180 new_pod
wait_for "workspace running again" 180 running_ok
chat_ok conv "after heartbeat restart" || fail "no answer after restart"
case "$(jq -r .reply "$body")" in
  echo\(*) ;;
  *) fail "unexpected reply $(jq -r .reply "$body")" ;;
esac
say "- replacement pod serving, history replayed ($(jq -r .reply "$body"))"
wait_for "unresponsive flag to clear" 60 flag_cleared

say "## audit trail"
ctl "$api/v1/audit?workspace=a1&limit=200" | jq -r '.[] | select(.action|test("upgrade|rollback|heartbeat")) | "\(.action) \(.result) \(.detail)"' | while read -r line; do say "- $line"; done
say "lifecycle e2e passed"
