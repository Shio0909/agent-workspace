#!/usr/bin/env bash
# Runs the agent against a real OpenAI-compatible endpoint, one model at a time,
# and prints per-scenario results. Nothing is written inside the repository.
#
#   LLM_BASE_URL   endpoint base, e.g. https://example.com/v1
#   LLM_KEY_FILE   file holding the API key (never pass the key on the command line)
#   MODELS         space-separated model ids
#   N, C           samples per scenario and concurrency (default 10, 4)
#   OUT_DIR        where raw outcomes are written (default: a fresh temp dir)
set -euo pipefail

: "${LLM_BASE_URL:?set LLM_BASE_URL}"
: "${LLM_KEY_FILE:?set LLM_KEY_FILE}"
: "${MODELS:?set MODELS}"
N="${N:-10}"
C="${C:-4}"
OUT_DIR="${OUT_DIR:-$(mktemp -d)}"
root="$(cd "$(dirname "$0")/.." && pwd)"
bin="$(mktemp -d)"
trap 'kill ${agent_pid:-} 2>/dev/null || true; rm -rf "$bin"' EXIT

(cd "$root" && go build -o "$bin/agent-runtime" ./cmd/agent-runtime && go build -o "$bin/agent-eval" ./cmd/agent-eval)
mkdir -p "$OUT_DIR"
echo "raw outcomes: $OUT_DIR"

for model in $MODELS; do
  work="$(mktemp -d)"
  creds="$(mktemp -d)"
  install -m 0600 "$LLM_KEY_FILE" "$creds/llm_api_key"
  port=$((20000 + RANDOM % 20000))
  WORKSPACE_DIR="$work" CREDENTIAL_DIR="$creds" LLM_BASE_URL="$LLM_BASE_URL" LLM_MODEL="$model" \
    LISTEN_ADDR="127.0.0.1:$port" "$bin/agent-runtime" >"$OUT_DIR/$model.agent.log" 2>&1 &
  agent_pid=$!
  for _ in $(seq 50); do curl -fs "http://127.0.0.1:$port/health" >/dev/null && break; sleep 0.1; done
  "$bin/agent-eval" -url "http://127.0.0.1:$port" -n "$N" -c "$C" -workspace-dir "$work" \
    -scenarios "${SCENARIOS:-}" -label "$model" -out "$OUT_DIR/$model.json" || echo "agent-eval exited $?"
  kill "$agent_pid" 2>/dev/null || true
  wait "$agent_pid" 2>/dev/null || true
  rm -rf "$work" "$creds"
done
