#!/usr/bin/env bash
set -euo pipefail

mode="${1:-optimized}"
shift || true
concurrencies=("$@")
if [ ${#concurrencies[@]} -eq 0 ]; then
  concurrencies=(20 100 200)
fi

cluster="${KIND_CLUSTER:-agent-workspace-bench}"
namespace="${NAMESPACE:-agent-workspace}"
context="kind-${cluster}"
token="${AGENT_WORKSPACE_TOKEN:-$(openssl rand -hex 16)}"
duration="${DURATION:-60s}"
observe_seconds="${OBSERVE_SECONDS:-10}"
cold_starts="${COLD_STARTS:-3}"
cold_settle_seconds="${COLD_SETTLE_SECONDS:-1}"
api_workspaces="${API_WORKSPACES:-0}"
ready_cache="${READY_CACHE:-true}"
results_dir="${RESULTS_DIR:-benchmark-results}"
# IMAGE selects the controller build under test; LABEL names the result files.
image="${IMAGE:-agent-workspace:local}"
label="${LABEL:-$mode}"
fortio_image="${FORTIO_IMAGE:-fortio/fortio:latest}"

case "$mode" in
  baseline|optimized) ;;
  *) echo "usage: $0 <baseline|optimized> [concurrency...]" >&2; exit 2 ;;
esac

mkdir -p "$results_dir"
if ! kind get clusters | grep -qx "$cluster"; then
  echo "kind cluster $cluster does not exist; create it with deploy/kind-benchmark.yaml" >&2
  exit 2
fi

app_node="$(kubectl --context "$context" get nodes -l agent-workspace/role=app -o jsonpath='{.items[0].metadata.name}')"
control_plane_node="$(kubectl --context "$context" get nodes -l agent-workspace/role=loadgen -o jsonpath='{.items[0].metadata.name}')"
kubectl --context "$context" taint node "$control_plane_node" agent-workspace/role=loadgen:NoSchedule --overwrite >/dev/null

cleanup() {
  kubectl --context "$context" -n "$namespace" delete namespace "$namespace" --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl --context "$context" create namespace "$namespace" >/dev/null
kubectl --context "$context" -n "$namespace" create secret generic agent-workspace-auth \
  --from-literal=token="$token" >/dev/null
kubectl --context "$context" -n "$namespace" create configmap agent-workspace-profiles \
  --from-file=profiles.json=configs/profiles.json >/dev/null
kubectl --context "$context" apply -f deploy/controller.yaml >/dev/null
kubectl --context "$context" -n "$namespace" patch deployment agent-workspace-controller --type=merge \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"agent-workspace/role":"app"}}}}}' >/dev/null
kubectl --context "$context" -n "$namespace" set image deployment/agent-workspace-controller controller="$image" >/dev/null
if [ "$mode" = "baseline" ]; then
  kubectl --context "$context" -n "$namespace" patch deployment agent-workspace-controller --type=json \
    -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"-kube-cache=false"}]' >/dev/null
fi
if [ "$ready_cache" = "false" ]; then
  kubectl --context "$context" -n "$namespace" patch deployment agent-workspace-controller --type=json \
    -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"-ready-cache=false"}]' >/dev/null
fi
kubectl --context "$context" -n "$namespace" rollout status deployment/agent-workspace-controller --timeout=120s >/dev/null

kubectl --context "$context" -n "$namespace" port-forward svc/agent-workspace-controller 8090:8090 >/tmp/agent-workspace-benchmark-port-forward.log 2>&1 &
forward_pid=$!
cleanup() {
  kill "$forward_pid" >/dev/null 2>&1 || true
  wait "$forward_pid" 2>/dev/null || true
  kubectl --context "$context" -n "$namespace" delete namespace "$namespace" --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8090/health >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS http://127.0.0.1:8090/health >/dev/null

curl -fsS -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
  -d '{"id":"demo","profile":"demo"}' http://127.0.0.1:8090/v1/workspaces >/dev/null
curl -fsS -X POST -H "X-Control-Token: $token" -H 'X-Biz-Id: benchmark-start' \
  http://127.0.0.1:8090/v1/workspaces/demo/start >/dev/null
curl -fsS -H "X-Control-Token: $token" http://127.0.0.1:8090/w/demo/health >/dev/null
if [ "$api_workspaces" -gt 0 ]; then
  for i in $(seq 1 "$api_workspaces"); do
    id="$(printf 'scale-%03d' "$i")"
    curl -fsS -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
      -d "{\"id\":\"$id\",\"profile\":\"demo\"}" http://127.0.0.1:8090/v1/workspaces >/dev/null
  done
fi

metrics_snapshot() {
  curl -fsS -H "X-Control-Token: $token" http://127.0.0.1:8090/metrics
}

metric_value() {
  local file="$1" key="$2"
  awk -v key="$key" '$1 == key { print $2; found=1 } END { if (!found) print 0 }' "$file"
}

metric_delta() {
  local before_file="$1" after_file="$2" key="$3"
  local before after
  before="$(metric_value "$before_file" "$key")"
  after="$(metric_value "$after_file" "$key")"
  awk -v after="$after" -v before="$before" 'BEGIN { printf "%.3f", after-before }'
}

workspace_phase() {
  curl -fsS -H "X-Control-Token: $token" http://127.0.0.1:8090/v1/workspaces/demo | jq -r '.phase // empty'
}

wait_for_phase() {
  local want="$1"
  for _ in $(seq 1 120); do
    if [ "$(workspace_phase 2>/dev/null || true)" = "$want" ]; then
      return 0
    fi
    sleep 0.5
  done
  echo "workspace did not reach phase $want" >&2
  return 1
}

# Measure steady-state API traffic after the initial informer LIST/WATCH has
# completed. Baseline performs direct GETs on scheduler rounds; optimized should
# have no API requests when nothing changes.
api_before="$results_dir/${label}-api-before.txt"
api_after="$results_dir/${label}-api-after.txt"
api_delta="$results_dir/${label}-api-delta.txt"
metrics_snapshot > "$api_before"
sleep "$observe_seconds"
metrics_snapshot > "$api_after"
awk 'FNR==NR { if ($1 ~ /^nc_kube_api_requests_total\{/) before[$1]=$2; next } $1 ~ /^nc_kube_api_requests_total\{/ { d=$2-(before[$1]+0); if (d>0) print $1, d }' \
  "$api_before" "$api_after" | sort > "$api_delta"
{
  echo "# ${label} Kubernetes API requests during ${observe_seconds}s steady state"
  echo
  if [ -s "$api_delta" ]; then
    echo '```text'
    cat "$api_delta"
    echo '```'
  else
    echo 'No Kubernetes API requests were observed.'
  fi
} > "$results_dir/${label}-api-calls.md"
cat "$results_dir/${label}-api-calls.md"

# Measure stop -> start -> first successful proxied request. The histogram
# values are from the same metric scrape, so wall time includes HTTP overhead.
cold_results="$results_dir/${label}-cold-start.tsv"
: > "$cold_results"
if [ "$cold_starts" -gt 0 ]; then
  for i in $(seq 1 "$cold_starts"); do
    stop_biz="bench-${label}-cold-stop-${i}"
    start_biz="bench-${label}-cold-start-${i}"
    curl -fsS -X POST -H "X-Control-Token: $token" -H "X-Biz-Id: $stop_biz" \
      http://127.0.0.1:8090/v1/workspaces/demo/stop >/dev/null
    wait_for_phase stopped

    metrics_snapshot > "$results_dir/${label}-cold-${i}-before.txt"
    curl -fsS -X POST -H "X-Control-Token: $token" -H "X-Biz-Id: $start_biz" \
      http://127.0.0.1:8090/v1/workspaces/demo/start >/dev/null
    wall_seconds="$(curl -fsS -o /dev/null -w '%{time_total}' -H "X-Control-Token: $token" \
      http://127.0.0.1:8090/w/demo/health)"
    sleep "$cold_settle_seconds"
    metrics_snapshot > "$results_dir/${label}-cold-${i}-after.txt"
    before_file="$results_dir/${label}-cold-${i}-before.txt"
    after_file="$results_dir/${label}-cold-${i}-after.txt"

    schedule_delta="$(metric_delta "$before_file" "$after_file" 'nc_workspace_start_seconds_sum{phase="schedule"}')"
    pull_delta="$(metric_delta "$before_file" "$after_file" 'nc_workspace_start_seconds_sum{phase="pull"}')"
    ready_delta="$(metric_delta "$before_file" "$after_file" 'nc_workspace_start_seconds_sum{phase="ready"}')"
    total_delta="$(metric_delta "$before_file" "$after_file" 'nc_workspace_start_seconds_sum{phase="total"}')"
    schedule_count="$(metric_delta "$before_file" "$after_file" 'nc_workspace_start_seconds_count{phase="schedule"}')"
    pull_count="$(metric_delta "$before_file" "$after_file" 'nc_workspace_start_seconds_count{phase="pull"}')"
    ready_count="$(metric_delta "$before_file" "$after_file" 'nc_workspace_start_seconds_count{phase="ready"}')"
    total_count="$(metric_delta "$before_file" "$after_file" 'nc_workspace_start_seconds_count{phase="total"}')"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$i" "$wall_seconds" "$schedule_delta" "$schedule_count" "$pull_delta" "$pull_count" \
      "$ready_delta" "$ready_count" "$total_delta" "$total_count" >> "$cold_results"
  done
fi
{
  echo "# ${label} cold starts"
  echo
  echo '| Run | Wall seconds | Schedule sum/n | Pull sum/n | Ready sum/n | Total sum/n |'
  echo '| ---: | ---: | ---: | ---: | ---: | ---: |'
  while IFS=$'\t' read -r run wall schedule schedule_n pull pull_n ready ready_n total total_n; do
    printf '| %s | %s | %s / %s | %s / %s | %s / %s | %s / %s |\n' \
      "$run" "$wall" "$schedule" "$schedule_n" "$pull" "$pull_n" "$ready" "$ready_n" "$total" "$total_n"
  done < "$cold_results"
} > "$results_dir/${label}-cold-start.md"
cat "$results_dir/${label}-cold-start.md"

# Newest pod: after "set image" the previous one may still be terminating.
controller_pod="$(kubectl --context "$context" -n "$namespace" get pod -l app=agent-workspace-controller --field-selector=status.phase=Running --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}')"
demo_pod="$(kubectl --context "$context" -n "$namespace" get pod -l agent-workspace/workspace=demo -o jsonpath='{.items[0].metadata.name}')"

for concurrency in "${concurrencies[@]}"; do
  result_base="$results_dir/${label}-${concurrency}"
  fortio_log="${result_base}.fortio.log"
  stats_log="${result_base}.stats.jsonl"
  : > "$stats_log"

  (
    while true; do
      kubectl --context "$context" get --raw "/api/v1/nodes/${app_node}/proxy/stats/summary" |
        jq -c --arg ts "$(date -u +%FT%TZ)" --arg c "$controller_pod" --arg d "$demo_pod" \
          '{ts:$ts,controller:(.pods[]|select(.podRef.name==$c)|{cpuNano:.cpu.usageNanoCores,memBytes:.memory.workingSetBytes}),demo:(.pods[]|select(.podRef.name==$d)|{cpuNano:.cpu.usageNanoCores,memBytes:.memory.workingSetBytes})}' \
          >> "$stats_log" 2>/dev/null || true
      sleep 5
    done
  ) &
  sampler_pid=$!

  overrides="$(jq -nc --arg node "$control_plane_node" '{spec:{nodeSelector:{"agent-workspace/role":"loadgen"},tolerations:[{key:"agent-workspace/role",operator:"Equal",value:"loadgen",effect:"NoSchedule"},{key:"node-role.kubernetes.io/control-plane",operator:"Exists",effect:"NoSchedule"}]}}')"
  kubectl --context "$context" -n "$namespace" run "fortio-${label}-${concurrency}" \
    --image="$fortio_image" --image-pull-policy=IfNotPresent --restart=Never --attach=true --rm -i \
    --overrides="$overrides" --command -- \
    /usr/bin/fortio load -c "$concurrency" -t "$duration" -qps 0 \
    -H "X-Control-Token: $token" \
    http://agent-workspace-controller.agent-workspace.svc.cluster.local:8090/w/demo/health \
    > "$fortio_log" 2>&1

  kill "$sampler_pid" >/dev/null 2>&1 || true
  wait "$sampler_pid" 2>/dev/null || true

  qps="$(grep -E 'All done [0-9]+ calls' "$fortio_log" | tail -1 || true)"
  p50="$(grep -m1 '# target 50%' "$fortio_log" | awk '{print $NF}' || true)"
  p90="$(grep -m1 '# target 90%' "$fortio_log" | awk '{print $NF}' || true)"
  p99="$(grep -m1 '# target 99%' "$fortio_log" | awk '{print $NF}' || true)"
  code200="$(grep -m1 '^Code 200' "$fortio_log" || true)"
  max_cpu="$(jq -s '[.[].controller.cpuNano // 0] | max' "$stats_log" 2>/dev/null || echo 0)"
  max_mem="$(jq -s '[.[].controller.memBytes // 0] | max' "$stats_log" 2>/dev/null || echo 0)"

  cat > "${result_base}.md" <<RESULT
# ${label} @ ${concurrency} concurrency

- image: ${image} (mode ${mode})
- duration: ${duration}
- path: /w/demo/health
- result: ${qps}
- P50: ${p50}s
- P90: ${p90}s
- P99: ${p99}s
- ${code200}
- controller max CPU: $(awk -v n="$max_cpu" 'BEGIN { printf "%.3f cores", n/1000000000 }')
- controller max memory: $(awk -v n="$max_mem" 'BEGIN { printf "%.1f MiB", n/1024/1024 }')
RESULT

  echo "--- ${label} ${concurrency} ---"
  cat "${result_base}.md"
done
