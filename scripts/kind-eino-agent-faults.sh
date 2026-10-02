#!/usr/bin/env bash
# Failure and load checks for the eino-agent profile against a real
# OpenAI-compatible endpoint, the ones scripts/kind-eino-agent.sh does not
# cover. Same inputs as that script (LLM_BASE_URL, LLM_MODEL, LLM_KEY_FILE) and
# the same images.
#
#   1. a streamed answer reaches the client incrementally through the gateway
#   2. PostgreSQL inside the pod is killed: does the workspace recover, and
#      does the session survive
#   3. the pod is killed without a grace period (no clean PostgreSQL stop):
#      the session is recovered from the volume and the token count never goes
#      backwards
#   4. concurrent turns inside the profile's 1Gi limit: no OOM kill, no restart
#   5. a knowledge base is created, a document uploaded and found by the agent
#   6. a second workspace on the same cluster is isolated from the first
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
ws=f1
sid=""
body="$(mktemp)"
profiles="$(mktemp)"
workdir="$(mktemp -d)"

say() { printf '%s\n' "$*"; [ -z "$results" ] || printf '%s\n' "$*" >> "$results"; }
ctl() { curl -fsS -H "X-Control-Token: $token" "$@"; }
fail() { echo "FAIL: $*" >&2; ctl "$api/v1/workspaces/$ws" 2>/dev/null | jq -c . >&2 || true; exit 1; }
now() { python3 -c 'import time;print(int(time.time()))'; }

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" >/dev/null 2>&1 || true; done
  rm -rf "$body" "$profiles" "$workdir"
  kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

jq --arg u "$LLM_BASE_URL" --arg m "$LLM_MODEL" \
  '.["eino-agent"].env.LLM_BASE_URL=$u | .["eino-agent"].env.LLM_MODEL=$m' configs/profiles.json > "$profiles"

kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl --context "$context" create namespace "$namespace" >/dev/null
$k create secret generic agent-workspace-auth --from-literal=token="$token" >/dev/null
$k create configmap agent-workspace-profiles --from-file=profiles.json="$profiles" >/dev/null
sed 's|"-idle", "2m"|"-idle", "10m", "-upgrade-settle", "5s", "-heartbeat-interval", "3s", "-reconcile", "2s"|' \
  deploy/controller.yaml | kubectl --context "$context" apply -f - >/dev/null
$k set image deployment/agent-workspace-controller controller="$controller_image" >/dev/null
$k rollout status deployment/agent-workspace-controller --timeout=120s >/dev/null
$k port-forward svc/agent-workspace-controller 8090:8090 >/tmp/kind-eino-faults-ctl.log 2>&1 &
pids+=($!)
for _ in $(seq 1 30); do curl -fsS $api/health >/dev/null 2>&1 && break; sleep 1; done

chat_to() { # workspace message -> http code; body in $body. Session id only for $ws.
  local w="$1" session=""
  [ "$w" != "$ws" ] || [ -z "$sid" ] || session=",\"session_id\":\"$sid\""
  curl -sS -m 170 -o "$body" -w '%{http_code}' -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
    -d "{\"query\":\"$2\",\"use_agent\":true$session}" "$api/w/$w/api/v1/chat" || true
}
chat() { chat_to "$ws" "$1"; }
chat_ok() { [ "$(chat "$1")" = 200 ]; }
w() { ctl "$api/v1/workspaces/${2:-$ws}" | jq -r "$1"; }
pod_heartbeat() { curl -fsS -H "X-Control-Token: $token" "$api/w/${1:-$ws}/heartbeat"; }
wait_for() { # description timeout command...
  local what="$1" limit="$2"; shift 2
  local deadline=$(( $(now) + limit ))
  until "$@" >/dev/null 2>&1; do
    [ "$(now)" -lt "$deadline" ] || fail "timed out after ${limit}s waiting for: $what"
    sleep 1
  done
}
pod_of() { $k get pod -l agent-workspace/workspace="${1:-$ws}" -o jsonpath='{.items[0].metadata.name}'; }
pod_uid() { $k get pod -l agent-workspace/workspace="${1:-$ws}" -o jsonpath='{.items[0].metadata.uid}'; }
put_key() { # workspace
  jq -Rs '{values:{llm_api_key:rtrimstr("\n")}}' "$LLM_KEY_FILE" | ctl -X PUT -H 'Content-Type: application/json' -d @- "$api/v1/workspaces/$1/credentials" >/dev/null
}
create_ws() { # id
  ctl -H 'Content-Type: application/json' -d "{\"id\":\"$1\",\"profile\":\"eino-agent\"}" $api/v1/workspaces >/dev/null
  put_key "$1"
  ctl -X POST -H "X-Biz-Id: faults-start-$1" $api/v1/workspaces/$1/start >/dev/null
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

create_ws $ws
wait_for "eino_agent to answer" 300 chat_ok "你好"
sid="$(jq -r .session_id "$body")"

say "## 1. a streamed answer reaches the client incrementally through the gateway"
python3 - "$api" "$token" "$ws" <<'PY' | tee "$workdir/stream.txt" >/dev/null
import http.client, json, sys, time
api, token, ws = sys.argv[1:4]
host, port = api.replace("http://", "").split(":")
c = http.client.HTTPConnection(host, int(port), timeout=170)
t0 = time.time()
c.request("POST", f"/w/{ws}/api/v1/chat/stream",
          json.dumps({"query": "写一段不少于 400 字的文字,解释为什么要对 API 密钥做定期轮换。", "use_agent": True}),
          {"Content-Type": "application/json", "X-Control-Token": token})
r = c.getresponse()
first = None; n = 0; chars = 0
while True:
    line = r.readline()
    if not line: break
    line = line.strip()
    if not line.startswith(b"data:"): continue
    try: ev = json.loads(line[5:].strip())
    except Exception: continue
    ty = ev.get("type")
    if ty == "content":
        n += 1; chars += len(ev.get("content") or "")
        if first is None: first = time.time() - t0
    if ty == "done": break
print(json.dumps({"status": r.status, "first_content_s": first, "total_s": time.time() - t0, "content_events": n, "chars": chars}))
PY
stream="$(tail -n 1 "$workdir/stream.txt")"
echo "$stream" | jq -e '.status == 200 and .content_events > 5 and .first_content_s != null and .first_content_s < .total_s * 0.8' >/dev/null \
  || fail "the answer was not streamed through the gateway: $stream"
say "- through the gateway: $(echo "$stream" | jq -r '"first content after \(.first_content_s*100|round/100)s of \(.total_s*100|round/100)s, \(.content_events) content events, \(.chars) characters"')"

say "## 2. PostgreSQL killed inside the pod"
stored_usage() { # the total the pod has committed to its own database
  $k exec "$(pod_of)" -- sh -c 'PGPASSWORD=$(cat /workspace/.eino/pg_password) /usr/lib/postgresql/17/bin/psql -h 127.0.0.1 -U eino -d eino -tAc "SELECT total_tokens FROM llm_usage_totals WHERE id=1"'
}
regressions() { ctl "$api/v1/audit?workspace=$ws&limit=300" | jq '[.[] | select(.action == "usage-regressed")] | length'; }
pg_crash() { # label seconds-between-the-turn-and-the-kill
  local label="$1" settle="$2" t pod_before hb stored regressed_before recovered=no after lost ctl_total killed_at restarts
  t="$(plant)"
  sleep "$settle"
  hb="$(pod_heartbeat | jq .usage.total_tokens)"
  stored="$(stored_usage)"
  regressed_before="$(regressions)"
  pod_before="$(pod_uid)"
  killed_at="$(now)"
  $k exec "$(pod_of)" -- sh -c 'kill -9 "$(head -n 1 /workspace/pgdata/postmaster.pid)"'
  # The profile has a readiness probe only. Give the platform a fair window to
  # notice before calling it a failure.
  for _ in $(seq 1 60); do
    if [ "$(chat 'ping')" = 200 ]; then recovered=yes; break; fi
    sleep 3
  done
  [ "$recovered" = yes ] || { say "- NOT recovered within 180s: the workspace stayed unusable after PostgreSQL died"; fail "PostgreSQL died and nothing brought the workspace back"; }
  restarts="$($k get pod -l agent-workspace/workspace=$ws -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')"
  say "- $label: answering again $(( $(now) - killed_at ))s after the kill, pod $( [ "$(pod_uid)" = "$pod_before" ] && echo kept || echo replaced ), container restarts=$restarts"
  if [ "$restarts" -gt 0 ]; then
    $k logs "$(pod_of)" --previous --tail=12 2>&1 | cut -c1-200 | while read -r line; do say "  previous container: $line"; done
  fi
  expect_recall "$t"
  say "- $label: the session survived the crash; the real model repeated the token"
  after="$(pod_heartbeat | jq .usage.total_tokens)"
  lost=$(( hb - stored ))
  [ "$lost" -ge 0 ] || fail "$label: the database holds more tokens ($stored) than the pod counted ($hb)"
  [ "$after" -ge "$stored" ] || fail "$label: a committed count was lost: $stored before the crash, $after after"
  ctl_total="$(w .usage.total_tokens)"
  [ "$ctl_total" -ge "$stored" ] || fail "$label: the controller's count went backwards: $stored to $ctl_total"
  # The turn had ended before the kill, so its tokens must already be committed:
  # the server flushes when a request ends instead of waiting for its timer.
  [ "$lost" = 0 ] || fail "$label: $lost tokens of a finished turn were not committed when the database was killed"
  [ "$after" -ge "$hb" ] || fail "$label: the count went from $hb to $after although everything had been flushed"
  [ "$(regressions)" = "$regressed_before" ] || fail "$label: the controller logged a usage regression although everything had been flushed"
  say "- $label: tokens counted in memory $hb, committed $stored (unflushed at the kill: $lost), after recovery $after, controller $ctl_total, new usage-regressed events: $(( $(regressions) - regressed_before ))"
}
pg_crash "crash 6s after a turn" 6
pg_crash "crash right after a turn" 0
ctl "$api/v1/audit?workspace=$ws&limit=40" | jq -r '.[] | select(.action|test("credential|start$|stop$")|not) | "  audit: \(.action) \(.result) \(.detail)"' | head -n 10 | while read -r line; do say "$line"; done
$k get events --field-selector involvedObject.kind=Pod --sort-by=.lastTimestamp -o custom-columns=REASON:.reason,MESSAGE:.message --no-headers | tail -n 6 | cut -c1-170 | while read -r line; do say "  event: $line"; done

say "## 3. pod killed without a grace period"
t3="$(plant)"
pod_count_before="$(pod_heartbeat | jq .usage.total_tokens)"
committed_before="$(stored_usage)"
controller_before="$(w .usage.total_tokens)"
pod_before="$(pod_uid)"
$k delete pod "$(pod_of)" --grace-period=0 --force >/dev/null 2>&1
replaced() { local u; u="$(pod_uid 2>/dev/null || true)"; [ -n "$u" ] && [ "$u" != "$pod_before" ]; }
wait_for "a replacement pod" 120 replaced
wait_for "the workspace to answer" 300 chat_ok "ping"
expect_recall "$t3"
pod_count_after="$(pod_heartbeat | jq .usage.total_tokens)"
controller_after="$(w .usage.total_tokens)"
# What the pod had committed must survive; the controller's own record must
# never go down. The pod's in-memory count at the kill is not a promise: a
# SIGKILL can take what a running turn had not yet written.
[ "$pod_count_after" -ge "$committed_before" ] || fail "committed tokens were lost: $committed_before before the kill, $pod_count_after after"
[ "$controller_after" -ge "$controller_before" ] || fail "the controller's count went backwards: $controller_before to $controller_after"
say "- killed with --grace-period=0, replaced, the session was recovered and the real model repeated the token; pod count in memory $pod_count_before, committed $committed_before, after $pod_count_after; controller $controller_before -> $controller_after"

say "## 4. concurrent turns inside the 1Gi limit"
pods="$(pod_of)"
codes="$workdir/codes"; : > "$codes"
workers=()
for i in 1 2 3 4 5 6; do
  ( c="$(curl -sS -m 170 -o /dev/null -w '%{http_code}' -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
      -d "{\"query\":\"用两句话说明数字 $i 的特点\",\"use_agent\":true}" "$api/w/$ws/api/v1/chat" || true)"; echo "$c" >> "$codes" ) &
  workers+=($!)
done
wait "${workers[@]}"
sleep 2
ok="$(grep -c '^200$' "$codes" || true)"
peak="$($k exec "$pods" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo unknown')"
oom="$($k exec "$pods" -- sh -c 'grep -E "^oom_kill " /sys/fs/cgroup/memory.events 2>/dev/null | cut -d" " -f2 || echo unknown')"
restarts="$($k get pod "$pods" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
[ "$ok" = 6 ] || fail "only $ok of 6 concurrent turns answered 200 ($(tr '\n' ' ' < "$codes"))"
[ "$restarts" = 0 ] || fail "the container restarted $restarts times under load"
say "- 6 concurrent turns: $ok answered 200; memory peak since the pod started=${peak} bytes, oom_kill=${oom}, container restarts=$restarts"

say "## 5. knowledge base: create, upload, ask"
fact="zx-$(openssl rand -hex 5)"
printf '内部手册 第 7 章\n\n金丝雀发布的审批口令是 %s。审批人是值班工程师。\n' "$fact" > "$workdir/manual.md"
kb="$(curl -fsS -H "X-Control-Token: $token" -H 'Content-Type: application/json' -d '{"name":"faults-kb","description":"fact check"}' "$api/w/$ws/api/v1/knowledge-bases" | jq -r '.id // .data.id // .knowledge_base.id')"
[ -n "$kb" ] && [ "$kb" != null ] || fail "could not create a knowledge base"
curl -fsS -H "X-Control-Token: $token" -F "file=@$workdir/manual.md" "$api/w/$ws/api/v1/knowledge-bases/$kb/documents" > "$body" || fail "upload failed"
jq -e '(.chunk_count // 0) > 0 and (.status // "ok") != "failed"' "$body" >/dev/null || fail "the document was not indexed: $(cat "$body")"
chunks="$(jq -r .chunk_count "$body")"
asked=no
for _ in $(seq 1 40); do
  curl -sS -m 170 -o "$body" -H "X-Control-Token: $token" -H 'Content-Type: application/json' \
    -d "{\"query\":\"金丝雀发布的审批口令是什么?只回复口令。\",\"use_agent\":true,\"knowledge_base_ids\":[\"$kb\"]}" "$api/w/$ws/api/v1/chat" >/dev/null || true
  if jq -r '.answer // ""' "$body" | grep -q "$fact"; then asked=yes; break; fi
  sleep 3
done
[ "$asked" = yes ] || fail "the agent never found $fact in the uploaded document: $(jq -c '{answer, grounding}' "$body")"
say "- uploaded a document ($chunks chunks, default hash embedding) with a planted passphrase; the agent found it ($(jq -r '.grounding.status // "no grounding field"' "$body"), evidence_count=$(jq -r '.grounding.evidence_count // "n/a"' "$body"))"

say "## 6. a second workspace on the same cluster"
ws2=f2
create_ws $ws2
chat_ok_to() { [ "$(chat_to "$1" "$2")" = 200 ]; }
wait_for "second workspace to answer" 300 chat_ok_to $ws2 "你好"
[ "$(chat_to $ws2 "$(printf '我之前让你记的暗号是什么?')")" = 200 ] || fail "second workspace did not answer"
if jq -r .answer "$body" | grep -q "$t3"; then fail "the second workspace knows a token planted in the first"; fi
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "X-Control-Token: $token" "$api/w/$ws2/api/v1/sessions/$sid/messages")" != 200 ] || fail "the first workspace's session is visible in the second"
pvc1="$($k get pvc nc-$ws -o jsonpath='{.metadata.uid}')"; pvc2="$($k get pvc nc-$ws2 -o jsonpath='{.metadata.uid}')"
[ "$pvc1" != "$pvc2" ] || fail "both workspaces share a volume"
usage_of() { w '.usage.total_tokens // 0' "$1"; }
usage_positive() { [ "$(usage_of "$1")" -gt 0 ]; }
wait_for "the second workspace's usage to be reported" 60 usage_positive $ws2
u1="$(usage_of $ws)"; u2="$(usage_of $ws2)"
[ "$u1" != "$u2" ] || fail "usage counts are identical ($u1)"
[ "$u2" -lt "$u1" ] || fail "the second workspace (two short turns) reports more tokens ($u2) than the first ($u1)"
say "- separate volumes, the second workspace cannot see the first's session or token; usage counts $u1 and $u2"

say "## audit"
ctl "$api/v1/audit?workspace=$ws&limit=300" | jq -r '.[] | select(.action|test("restart|heartbeat|unhealthy|unready|replace|usage")) | "\(.action) \(.result) \(.detail)"' | while read -r line; do say "- $line"; done
say "faults e2e passed"
