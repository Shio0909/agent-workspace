#!/usr/bin/env bash
set -euo pipefail

namespace="${NAMESPACE:-agent-workspace}"
cluster="${KIND_CLUSTER:-agent-workspace}"
context="kind-${cluster}"
token="${AGENT_WORKSPACE_TOKEN:-0123456789abcdef0123456789abcdef}"

kubectl --context "$context" create namespace "$namespace" --dry-run=client -o yaml | kubectl --context "$context" apply -f -
kubectl --context "$context" -n "$namespace" create secret generic agent-workspace-auth \
  --from-literal=token="$token" --dry-run=client -o yaml | kubectl --context "$context" apply -f -
kubectl --context "$context" -n "$namespace" create configmap agent-workspace-profiles \
  --from-file=profiles.json=configs/profiles.json --dry-run=client -o yaml | kubectl --context "$context" apply -f -
kubectl --context "$context" apply -f deploy/controller.yaml
kubectl --context "$context" -n "$namespace" rollout status deployment/agent-workspace-controller --timeout=120s

kubectl --context "$context" -n "$namespace" port-forward svc/agent-workspace-controller 8090:8090 >/tmp/agent-workspace-port-forward.log 2>&1 &
forward_pid=$!
cleanup() {
  kill "$forward_pid" >/dev/null 2>&1 || true
  kubectl --context "$context" delete namespace "$namespace" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8090/health >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS http://127.0.0.1:8090/health >/dev/null

curl -fsS -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
  -d '{"id":"demo","profile":"demo"}' http://127.0.0.1:8090/v1/workspaces >/dev/null
curl -fsS -X POST -H "X-Control-Token: $token" -H 'X-Biz-Id: smoke-start' \
  http://127.0.0.1:8090/v1/workspaces/demo/start >/dev/null
curl -fsS -H "X-Control-Token: $token" http://127.0.0.1:8090/w/demo/health >/dev/null
curl -fsS -X PUT -H "X-Control-Token: $token" --data-binary 'hello persistent workspace' \
  http://127.0.0.1:8090/w/demo/note >/dev/null
test "$(curl -fsS -H "X-Control-Token: $token" http://127.0.0.1:8090/w/demo/note)" = "hello persistent workspace"

echo "agent-workspace kind smoke test passed"
