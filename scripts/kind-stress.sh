#!/usr/bin/env bash
# Failure and load scenarios on a kind cluster, with the real controller and
# real agent pods. Every scenario states the invariant it checks and exits
# non-zero when it does not hold.
#
#   A. credential rotation while many workspaces are being used: with the
#      provider accepting old and new keys during the overlap, no request fails;
#   B. kill -9 of the agent process under load: turns that were answered are
#      never lost, and the history stays readable;
#   B2. kill -9 in the middle of one slow turn: that turn is not half-saved;
#   C. kill -9 of the controller under load: workloads keep running, the
#      controller resumes from its state volume and traffic returns;
#   E. a profile with restart_on_credential_change: a rotation replaces the pod
#      and the conversation survives on the volume;
#   D. (optional) the same rotation against a real OpenAI-compatible provider.
#
# Images to load first: see scripts/kind-lifecycle.sh. For D set
# REAL_LLM_URL (ending in /v1), REAL_LLM_MODEL and REAL_LLM_KEY_FILE; the key is
# read from the file, passed on stdin, and lives only in a Secret of the
# throw-away namespace.
set -euo pipefail

namespace="${NAMESPACE:-agent-workspace}"
cluster="${KIND_CLUSTER:-agent-workspace}"
context="kind-${cluster}"
controller_image="${CONTROLLER_IMAGE:-agent-workspace:local}"
token="${AGENT_WORKSPACE_TOKEN:-$(openssl rand -hex 16)}"
results="${RESULTS_FILE:-}"
count="${WORKSPACES:-8}"
loops="${LOOPS_PER_WORKSPACE:-3}"
rounds="${ROTATIONS:-3}"
kills="${AGENT_KILLS:-8}"
load_seconds="${KILL_LOAD_SECONDS:-100}"
only="${SCENARIOS:-A B B2 C E D}"
k="kubectl --context $context -n $namespace"
api=http://127.0.0.1:8090
llm=http://127.0.0.1:8081
work="$(mktemp -d)"

say() { printf '%s\n' "$*"; [ -z "$results" ] || printf '%s\n' "$*" >> "$results"; }
ctl() { curl -fsS -H "X-Control-Token: $token" "$@"; }
fail() { say "FAIL: $*"; exit 1; }
now() { python3 -c 'import time;print(int(time.time()))'; }
wants() { [[ " $only " == *" $1 "* ]]; }

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do pkill -P "$p" >/dev/null 2>&1 || true; kill "$p" >/dev/null 2>&1 || true; done
  rm -rf "$work"
  kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

profiles="$work/profiles.json"
jq '. + {"agent-restart": (.agent | .restart_on_credential_change=true)}' configs/profiles.json > "$profiles"
if [ -n "${REAL_LLM_URL:-}" ]; then
  jq --arg url "$REAL_LLM_URL" --arg model "${REAL_LLM_MODEL:?REAL_LLM_MODEL is required}" \
    '. + {"agent-real": (.agent | .env.LLM_BASE_URL=$url | .env.LLM_MODEL=$model)}' "$profiles" > "$profiles.new" && mv "$profiles.new" "$profiles"
fi

kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl --context "$context" create namespace "$namespace" >/dev/null
$k create secret generic agent-workspace-auth --from-literal=token="$token" >/dev/null
$k create configmap agent-workspace-profiles --from-file=profiles.json="$profiles" >/dev/null
sed 's|"-idle", "2m"|"-idle", "6h", "-heartbeat-interval", "5s", "-reconcile", "3s"|' deploy/controller.yaml | kubectl --context "$context" apply -f - >/dev/null
$k set image deployment/agent-workspace-controller controller="$controller_image" >/dev/null
kubectl --context "$context" apply -f deploy/fake-llm.yaml >/dev/null
$k rollout status deployment/agent-workspace-controller --timeout=120s >/dev/null
$k rollout status deployment/fake-llm --timeout=120s >/dev/null

# Port-forwards die with the pod they point at, and scenario C kills the
# controller, so keep them alive in a loop.
forward() { # service local-port remote-port
  ( while true; do $k port-forward "svc/$1" "$2:$3" >/dev/null 2>&1 || true; sleep 0.5; done ) &
  pids+=($!)
}
forward agent-workspace-controller 8090 8090
forward fake-llm 8081 8081
for _ in $(seq 1 30); do curl -fsS $api/health >/dev/null 2>&1 && curl -fsS $llm/health >/dev/null 2>&1 && break; sleep 1; done

keys() { # key... -> provider accepts exactly these
  local json; json="$(printf '%s\n' "$@" | jq -R . | jq -sc '{keys: .}')"
  curl -fsS -X PUT -d "$json" $llm/admin/keys >/dev/null
}
stat() { curl -fsS $llm/admin/stats | jq -r ".$1"; }
chat() { # workspace session message [curl-timeout] -> "code version reply" ; never fails
  local out
  out="$(curl -sS -m "${4:-40}" -w '\n%{http_code}' -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
    -d "$(jq -nc --arg s "$2" --arg m "$3" '{session:$s,message:$m}')" "$api/w/$1/v1/chat" 2>/dev/null)" || { echo "000 0 -"; return 0; }
  local code="${out##*$'\n'}" body="${out%$'\n'*}"
  echo "$code $(jq -r '(.credential_version // 0)' <<<"$body" 2>/dev/null || echo 0) $(jq -r '(.reply // .error // "-")' <<<"$body" 2>/dev/null | tr '\n' ' ')"
}
wait_for() { # description timeout command...
  local what="$1" limit="$2"; shift 2
  local deadline=$(( $(now) + limit ))
  until "$@" >/dev/null 2>&1; do
    [ "$(now)" -lt "$deadline" ] || fail "timed out after ${limit}s waiting for: $what"
    sleep 1
  done
}
answers() { [ "$(chat "$1" probe ping | cut -d' ' -f1)" = 200 ]; }
version_of() { chat "$1" probe ping | cut -d' ' -f2; }
set_key() { # workspace key
  jq -nc --arg k "$2" '{values:{llm_api_key:$k}}' | ctl -X PUT -H 'Content-Type: application/json' -d @- "$api/v1/workspaces/$1/credentials" >/dev/null
}
node_of() { $k get pod -l "agent-workspace/workspace=$1" -o jsonpath='{.items[0].spec.nodeName}'; }
pod_uid() { $k get pod -l "agent-workspace/workspace=$1" -o jsonpath='{.items[0].metadata.uid}'; }
restarts() { $k get pod -l "agent-workspace/workspace=$1" -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}'; }
# Kill the agent process of one workspace only: the container's PID as the
# node sees it, not a name match, which would hit every agent on that node.
kill9() {
  local node cid pid
  node="$(node_of "$1")"
  cid="$(docker exec "$node" crictl ps -q --label "io.kubernetes.pod.name=$(pod_name "$1")" --state running | head -1)"
  [ -n "$cid" ] || return 1
  pid="$(docker exec "$node" crictl inspect --output go-template --template '{{.info.pid}}' "$cid")"
  docker exec "$node" kill -9 "$pid"
}
pod_name() { $k get pod -l "agent-workspace/workspace=$1" -o jsonpath='{.items[0].metadata.name}'; }

loadgen() { # workspace loop-index end-epoch
  local n=0 line
  while [ "$(now)" -lt "$3" ]; do
    n=$((n+1))
    line="$(chat "$1" "$phase-$2" "m$n" 30)"
    echo "$1 $phase-$2 ${line%% *} $(cut -d' ' -f2 <<<"$line")" >> "$work/load.$1.$2"
  done
}
start_load() { # seconds
  # A new session name per phase: the replay check counts every turn a session
  # has ever had, so sessions must not carry turns over from an earlier phase.
  phase="ld$RANDOM"
  local end=$(( $(now) + $1 )) w i
  rm -f "$work"/load.*
  load_pids=()
  for w in $(seq 1 "$count"); do for i in $(seq 1 "$loops"); do
    loadgen "w$w" "$i" "$end" & load_pids+=($!)
  done; done
}
stop_load() { for p in "${load_pids[@]}"; do wait "$p" || true; done; }
tally() { # prints "total ok failed codes"
  cat "$work"/load.* 2>/dev/null | awk '{t++; c[$3]++; if($3==200) ok++} END{printf "%d %d %d", t, ok, t-ok; for(k in c) if(k!=200) printf " [%s x%d]", k, c[k]}'
}

say "## setup: $count workspaces"
for w in $(seq 1 "$count"); do
  ctl -H 'Content-Type: application/json' -d "{\"id\":\"w$w\",\"profile\":\"agent\"}" $api/v1/workspaces >/dev/null
  set_key "w$w" key-v1
done
keys key-v1
for w in $(seq 1 "$count"); do ctl -X POST -H "X-Biz-Id: stress-start-$w" $api/v1/workspaces/w$w/start >/dev/null; done
for w in $(seq 1 "$count"); do wait_for "w$w to answer" 240 answers "w$w"; done
for w in $(seq 1 "$count"); do pod_uid "w$w" > "$work/uid.w$w"; done
say "- all workspaces answering"

if wants A; then
  say "## A. rotation under load ($count workspaces x $loops loops, $rounds rotations, overlap window)"
  rejected0="$(stat rejected)"
  start_load $(( rounds * 25 + 20 ))
  prev=key-v1
  for r in $(seq 1 "$rounds"); do
    sleep 8
    new="key-v$((r+1))"
    keys "$prev" "$new"
    t0=$(now)
    puts=()
    for w in $(seq 1 "$count"); do set_key "w$w" "$new" & puts+=($!); done
    for p in "${puts[@]}"; do wait "$p"; done
    wait_all() { for w in $(seq 1 "$count"); do [ "$(version_of "w$w")" = "$((r+1))" ] || return 1; done; }
    wait_for "all workspaces on key version $((r+1))" 180 wait_all
    say "- round $r: every workspace serving credential_version=$((r+1)) after $(( $(now) - t0 )) s"
    keys "$new"
    prev="$new"
  done
  stop_load
  read -r total ok failed rest < <(tally) || true
  say "- requests: total=$total ok=$ok failed=$failed ${rest:-}; provider rejected (401) delta=$(( $(stat rejected) - rejected0 ))"
  moved=0
  for w in $(seq 1 "$count"); do [ "$(pod_uid w$w)" = "$(cat "$work/uid.w$w")" ] && [ "$(restarts w$w)" = 0 ] || moved=$((moved+1)); done
  say "- workspaces whose pod was replaced or restarted during rotation: $moved"
  [ "$failed" = 0 ] || fail "A: $failed requests failed although old and new keys overlapped"
  [ "$moved" = 0 ] || fail "A: rotation restarted $moved pods"
  say "- invariant held: no failed request, no restart"
fi

if wants B2; then
  say "## B2. kill -9 in the middle of one slow turn"
  keys key-v1 key-v2 key-v3 key-v4 key-v5
  set_key w1 key-v1
  wait_for "w1 to pick up the key" 120 answers w1
  s="mid-$RANDOM"
  chat w1 "$s" "first" >/dev/null; chat w1 "$s" "second" >/dev/null
  r0="$(restarts w1)"
  ( chat w1 "$s" "sleep 25000 slow-turn" 60 > "$work/slow.out" ) &
  slow=$!
  sleep 4
  kill9 w1 || fail "B2: could not kill the agent process"
  wait "$slow" || true
  code="$(cut -d' ' -f1 "$work/slow.out")"
  say "- in-flight request ended with HTTP $code (not 200)"
  [ "$code" != 200 ] || fail "B2: the killed turn reported success"
  t0=$(now)
  wait_for "w1 to answer again" 120 answers w1
  say "- answering again $(( $(now) - t0 )) s after the kill; container restarts $r0 -> $(restarts w1), same pod"
  after="$(chat w1 "$s" third)"
  say "- next turn: ${after#* * }"
  case "${after#* * }" in
    "echo(3): third"*) say "- invariant held: history is exactly the two answered turns plus the new one" ;;
    *) fail "B2: unexpected history after kill: $after" ;;
  esac
fi

if wants B; then
  say "## B. kill -9 of the agent under load ($kills kills in about ${load_seconds}s)"
  start_load "$load_seconds"
  sleep 5
  hits=0
  for i in $(seq 1 "$kills"); do
    w="w$(( RANDOM % count + 1 ))"
    kill9 "$w" && hits=$((hits+1)) || true
    sleep $(( load_seconds / (kills + 1) - 1 ))
  done
  stop_load
  read -r total ok failed rest < <(tally) || true
  say "- requests during the run: total=$total ok=$ok failed=$failed ${rest:-} (failures are expected while a process is down)"
  say "- kills delivered: $hits"
  bad=0
  for w in $(seq 1 "$count"); do wait_for "w$w to answer" 180 answers "w$w"; done
  for w in $(seq 1 "$count"); do
    for i in $(seq 1 "$loops"); do
      f="$work/load.w$w.$i"; [ -f "$f" ] || continue
      okn="$(awk '$3==200' "$f" | wc -l | tr -d ' ')"; fn="$(awk '$3!=200' "$f" | wc -l | tr -d ' ')"
      n="$(chat "w$w" "$phase-$i" final | cut -d' ' -f3- | sed -n 's/^echo(\([0-9]*\)):.*/\1/p')"
      # Every answered turn is on disk, and a failed one may or may not be.
      if [ -z "$n" ] || [ "$n" -lt $((okn+1)) ] || [ "$n" -gt $((okn+fn+1)) ]; then
        say "  VIOLATION w$w session $phase-$i: replay says $n user turns, answered=$okn failed=$fn"; bad=$((bad+1))
      fi
    done
  done
  say "- sessions checked against 'answered <= persisted <= answered+failed': $((count*loops)), violations: $bad"
  total_restarts=0; for w in $(seq 1 "$count"); do total_restarts=$((total_restarts + $(restarts "w$w"))); done
  say "- total container restarts across workspaces: $total_restarts"
  [ "$bad" = 0 ] || fail "B: acknowledged turns were lost or sessions corrupted"
  say "- invariant held"
fi

if wants C; then
  say "## C. kill -9 of the controller under load"
  keys key-v1 key-v2 key-v3 key-v4 key-v5
  for w in $(seq 1 "$count"); do set_key "w$w" key-v1; done
  for w in $(seq 1 "$count"); do wait_for "w$w to answer" 120 answers "w$w"; done
  ids_before="$(ctl $api/v1/workspaces | jq -r '[.[].id]|sort|join(",")')"
  uids_before="$(for w in $(seq 1 "$count"); do pod_uid w$w; done | sort | tr '\n' ,)"
  start_load 60
  sleep 10
  ctrl_pod="$($k get pod -l app=agent-workspace-controller -o jsonpath='{.items[0].metadata.name}')"
  t0=$(now)
  $k delete pod "$ctrl_pod" --grace-period=0 --force >/dev/null 2>&1
  wait_for "controller to answer again" 120 curl -fsS $api/ready
  say "- controller ready again $(( $(now) - t0 )) s after the kill"
  stop_load
  read -r total ok failed rest < <(tally) || true
  say "- requests during the run: total=$total ok=$ok failed=$failed ${rest:-}"
  ids_after="$(ctl $api/v1/workspaces | jq -r '[.[].id]|sort|join(",")')"
  uids_after="$(for w in $(seq 1 "$count"); do pod_uid w$w; done | sort | tr '\n' ,)"
  [ "$ids_before" = "$ids_after" ] || fail "C: workspace list changed: $ids_before -> $ids_after"
  [ "$uids_before" = "$uids_after" ] || fail "C: workload pods were replaced by the controller restart"
  for w in $(seq 1 "$count"); do wait_for "w$w to answer" 120 answers "w$w"; done
  say "- invariant held: same $count workspaces, no pod replaced, all answering"
fi

if wants E; then
  say "## E. restart_on_credential_change: rotation replaces the pod, history stays"
  keys key-v1
  ctl -H 'Content-Type: application/json' -d '{"id":"re1","profile":"agent-restart"}' $api/v1/workspaces >/dev/null
  set_key re1 key-v1
  ctl -X POST -H 'X-Biz-Id: stress-start-re1' $api/v1/workspaces/re1/start >/dev/null
  wait_for "re1 to answer" 240 answers re1
  chat re1 conv one >/dev/null; chat re1 conv two >/dev/null
  uid="$(pod_uid re1)"; vol="$($k get pvc nc-re1 -o jsonpath='{.metadata.uid}')"
  keys key-v1 key-v2
  t0=$(now)
  set_key re1 key-v2
  replaced() { [ "$(pod_uid re1)" != "$uid" ] && answers re1; }
  wait_for "the pod to be replaced and answering" 240 replaced
  say "- pod replaced and answering $(( $(now) - t0 )) s after the rotation; credential_version=$(version_of re1)"
  keys key-v2
  after="$(chat re1 conv three)"
  say "- next turn on the new pod: ${after#* * }"
  [ "$($k get pvc nc-re1 -o jsonpath='{.metadata.uid}')" = "$vol" ] || fail "E: volume was replaced"
  case "${after#* * }" in
    "echo(3): three"*) say "- invariant held: new pod, same volume, history replayed, new key accepted" ;;
    *) fail "E: unexpected reply after restart: $after" ;;
  esac
fi

if wants D; then
  if [ -z "${REAL_LLM_URL:-}" ] || [ ! -r "${REAL_LLM_KEY_FILE:-/nonexistent}" ]; then
    say "## D. skipped (REAL_LLM_URL / REAL_LLM_KEY_FILE not set)"
  else
    say "## D. rotation against a real provider (model ${REAL_LLM_MODEL})"
    ctl -H 'Content-Type: application/json' -d '{"id":"real1","profile":"agent-real"}' $api/v1/workspaces >/dev/null
    set_key real1 invalid-key
    ctl -X POST -H 'X-Biz-Id: stress-start-real' $api/v1/workspaces/real1/start >/dev/null
    wait_for "real1 to be running" 240 bash -c "[ \"\$(curl -s -H 'X-Control-Token: $token' $api/v1/workspaces/real1 | jq -r .phase)\" = running ]"
    sleep 5
    uid="$(pod_uid real1)"
    for cycle in 1 2; do
      first="$(chat real1 "real-$cycle" "Reply with the single word: pong" 120)"
      say "- cycle $cycle, invalid key: HTTP ${first%% *}"
      [ "${first%% *}" != 200 ] || fail "D: invalid key was accepted"
      jq -Rsc '{values:{llm_api_key:(. | rtrimstr("\n"))}}' "$REAL_LLM_KEY_FILE" | ctl -X PUT -H 'Content-Type: application/json' -d @- $api/v1/workspaces/real1/credentials >/dev/null
      t0=$(now)
      good_reply() { local o; o="$(chat real1 "real-$cycle" "Reply with the single word: pong" 120)"; [ "${o%% *}" = 200 ]; }
      wait_for "real key to be used" 240 good_reply
      say "- cycle $cycle, real key delivered: first success after $(( $(now) - t0 )) s"
      set_key real1 invalid-key
      sleep 8
    done
    [ "$(pod_uid real1)" = "$uid" ] && [ "$(restarts real1)" = 0 ] || fail "D: pod restarted during rotation"
    say "- invariant held: same pod, zero restarts, invalid -> real -> invalid -> real"
  fi
fi

say "stress scenarios passed"
