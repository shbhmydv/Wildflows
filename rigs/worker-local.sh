#!/usr/bin/env bash
# ScriptRig adapter for an OpenAI-compatible local server (llama.cpp/nginx).
set -euo pipefail

worktree=""; prompt=""; log_dir=""; handle_out=""; timeout=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worktree) worktree="$2"; shift 2 ;;
    --prompt) prompt="$2"; shift 2 ;;
    --log-dir) log_dir="$2"; shift 2 ;;
    --handle-out) handle_out="$2"; shift 2 ;;
    --timeout) timeout="$2"; shift 2 ;;
    *) echo "worker-local: unknown argument: $1" >&2; exit 2 ;;
  esac
done
for required in worktree prompt log_dir handle_out; do
  [[ -n "${!required}" ]] || { echo "worker-local: missing --${required//_/-}" >&2; exit 2; }
done
worktree="$(cd "$worktree" && pwd)"
mkdir -p "$log_dir" "$(dirname "$handle_out")"
log_dir="$(cd "$log_dir" && pwd)"
handle_out="$(cd "$(dirname "$handle_out")" && pwd)/$(basename "$handle_out")"
echo "$(ps -o pgid= -p $$ | tr -d '[:space:]')" > "$handle_out"
export GIT_CEILING_DIRECTORIES="$(dirname "$worktree")"

url="${WILDFLOWS_LOCAL_URL:-http://127.0.0.1:8080/v1/chat/completions}"
model="${WILDFLOWS_LOCAL_MODEL:-local}"
request="$log_dir/request.json"; response="$log_dir/response.json"; err="$log_dir/curl.stderr.log"
python3 -c 'import json,sys; print(json.dumps({"model":sys.argv[1],"messages":[{"role":"user","content":sys.stdin.read()}]}))' \
  "$model" < "$prompt" > "$request"
headers=(-H 'Content-Type: application/json')
curl_args=(--silent --show-error --fail-with-body)
[[ -n "$timeout" ]] && curl_args+=(--max-time "$timeout")

set +e
(
  cd "$worktree"
  curl "${curl_args[@]}" "${headers[@]}" --data-binary "@$request" "$url" \
    > "$response" 2> "$err"
)
rc=$?
set -e
if [[ "$rc" -ne 0 ]]; then
  tail -c 65536 "$err" >&2 || true
  tail -c 65536 "$response" >&2 || true
  echo "worker-local: curl exited $rc ($url)" >&2
  exit "$rc"
fi
python3 -c '
import json, sys
data = json.load(sys.stdin)
try:
    text = data["choices"][0]["message"]["content"]
except (KeyError, IndexError, TypeError) as exc:
    raise SystemExit(f"worker-local: malformed completion response: {exc}") from exc
if not isinstance(text, str):
    raise SystemExit("worker-local: completion content is not text")
print(text, end="")
' < "$response"
