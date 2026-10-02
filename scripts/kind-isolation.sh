#!/usr/bin/env bash
# Pod Security and network isolation, checked on a real cluster.
#
#   1. The namespace enforces the "restricted" Pod Security profile. The
#      controller and the workspace pods must still be admitted, and a plain pod
#      must be refused (otherwise the enforcement checked nothing).
#   2. Workspace pods accept traffic only from the controller: another
#      workspace and an unrelated pod cannot reach one, the gateway can. The
#      policy is removed and put back to show that the block comes from it.
#
# Needs the controller and demo images loaded into the kind cluster and a CNI
# that enforces NetworkPolicy (kind's default does in current releases). Without
# enforcement the first blocked request below fails the script, it does not pass.
set -euo pipefail

namespace="${NAMESPACE:-agent-workspace}"
cluster="${KIND_CLUSTER:-agent-workspace}"
context="kind-${cluster}"
token="${AGENT_WORKSPACE_TOKEN:-$(openssl rand -hex 16)}"
k="kubectl --context $context -n $namespace"
api=http://127.0.0.1:8090
forward_pid=""
policy="$(mktemp)"

fail() { echo "FAIL: $*" >&2; exit 1; }
cleanup() {
  [ -z "$forward_pid" ] || kill "$forward_pid" >/dev/null 2>&1 || true
  rm -f "$policy"
  kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT
ctl() { curl -fsS -H "X-Control-Token: $token" "$@"; }
now() { python3 -c 'import time;print(int(time.time()))'; }
wait_for() { # description timeout command...
  local what="$1" limit="$2"; shift 2
  local deadline=$(( $(now) + limit ))
  until "$@" >/dev/null 2>&1; do
    [ "$(now)" -lt "$deadline" ] || fail "timed out after ${limit}s waiting for: $what"
    sleep 1
  done
}

kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl --context "$context" create namespace "$namespace" >/dev/null
kubectl --context "$context" label namespace "$namespace" \
  pod-security.kubernetes.io/enforce=restricted pod-security.kubernetes.io/enforce-version=latest >/dev/null
$k create secret generic agent-workspace-auth --from-literal=token="$token" >/dev/null
$k create configmap agent-workspace-profiles --from-file=profiles.json=configs/profiles.json >/dev/null
kubectl --context "$context" apply -f deploy/controller.yaml >/dev/null
$k rollout status deployment/agent-workspace-controller --timeout=120s >/dev/null \
  || fail "the controller pod was not admitted under the restricted profile: $($k get events | tail -n 3)"
echo "the controller runs in a namespace that enforces the restricted Pod Security profile"

$k port-forward svc/agent-workspace-controller 8090:8090 >/tmp/kind-isolation-ctl.log 2>&1 &
forward_pid=$!
wait_for "the controller API" 30 curl -fsS $api/health

if printf 'apiVersion: v1\nkind: Pod\nmetadata: {name: plain}\nspec:\n  containers: [{name: c, image: agent-workspace:local, command: [sleep, "60"]}]\n' \
   | $k apply -f - >/tmp/kind-isolation-plain.log 2>&1; then
  fail "a pod without a security context was admitted, so the namespace does not enforce Pod Security"
fi
grep -q "violates PodSecurity" /tmp/kind-isolation-plain.log || fail "the plain pod was refused for another reason: $(cat /tmp/kind-isolation-plain.log)"
echo "a pod without a security context is refused by admission"

for id in a b; do
  ctl -H 'Content-Type: application/json' -d "{\"id\":\"$id\",\"profile\":\"demo\"}" $api/v1/workspaces >/dev/null
  ctl -X POST -H "X-Biz-Id: isolation-start-$id" $api/v1/workspaces/$id/start >/dev/null
done
gateway_ok() { ctl -m 5 "$api/w/$1/health"; }
wait_for "workspace a behind the gateway" 180 gateway_ok a
wait_for "workspace b behind the gateway" 180 gateway_ok b
echo "both workspace pods were admitted and answer through the gateway"

cat <<'EOF' | $k apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: intruder}
spec:
  automountServiceAccountToken: false
  securityContext: {runAsNonRoot: true, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: c
      image: agent-workspace:local
      imagePullPolicy: IfNotPresent
      command: ["sleep", "3600"]
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: ["ALL"]}}
EOF
$k wait --for=condition=Ready pod/intruder --timeout=90s >/dev/null || fail "the intruder pod did not start"
pod_of() { $k get pod -l agent-workspace/workspace="$1" -o jsonpath='{.items[0].metadata.name}'; }
ip_a="$($k get pod "$(pod_of a)" -o jsonpath='{.status.podIP}')"
target="http://$ip_a:8080/health"
service="http://nc-a.$namespace.svc.cluster.local:8080/health"
reach() { # from-pod url
  $k exec "$1" -- wget -q -O /dev/null -T 3 "$2"
}

reach_blocked() { ! reach "$1" "$2"; }
wait_for "an unrelated pod to be blocked from a workspace pod" 60 reach_blocked intruder "$target"
reach_blocked intruder "$service" || fail "an unrelated pod reached workspace a through its Service"
reach_blocked "$(pod_of b)" "$target" || fail "workspace b reached workspace a"
gateway_ok a >/dev/null || fail "the gateway can no longer reach workspace a"
echo "an unrelated pod and a second workspace are blocked from workspace a (by pod IP and by Service); the gateway still reaches it"

$k get networkpolicy agent-workspace-ingress -o yaml > "$policy"
$k delete networkpolicy agent-workspace-ingress >/dev/null
reach_open() { reach intruder "$target"; }
wait_for "the intruder to reach the workspace once the policy is gone" 60 reach_open
echo "control: without the policy the same request succeeds"
$k apply -f "$policy" >/dev/null 2>&1 || { sed '/^  resourceVersion:/d;/^  uid:/d;/^  creationTimestamp:/d' "$policy" | $k apply -f - >/dev/null; }
wait_for "the policy to block the intruder again" 60 reach_blocked intruder "$target"
gateway_ok a >/dev/null || fail "the gateway lost workspace a after the policy came back"
echo "control: with the policy back the request is blocked again and the gateway still works"
echo "isolation e2e passed"
