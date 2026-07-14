#!/usr/bin/env bash
# ScriptRig adapter: pi/picodex planner output -> one validated JSON object.
set -euo pipefail

worktree=""; prompt=""; log_dir=""; handle_out=""; timeout=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worktree) worktree="$2"; shift 2 ;;
    --prompt) prompt="$2"; shift 2 ;;
    --log-dir) log_dir="$2"; shift 2 ;;
    --handle-out) handle_out="$2"; shift 2 ;;
    --timeout) timeout="$2"; shift 2 ;;
    *) echo "planner-picodex: unknown argument: $1" >&2; exit 2 ;;
  esac
done
for required in worktree prompt log_dir handle_out; do
  [[ -n "${!required}" ]] || { echo "planner-picodex: missing --${required//_/-}" >&2; exit 2; }
done
worktree="$(cd "$worktree" && pwd)"
mkdir -p "$log_dir" "$(dirname "$handle_out")"
log_dir="$(cd "$log_dir" && pwd)"
handle_out="$(cd "$(dirname "$handle_out")" && pwd)/$(basename "$handle_out")"
echo "$(ps -o pgid= -p $$ | tr -d '[:space:]')" > "$handle_out"
export GIT_CEILING_DIRECTORIES="$(dirname "$worktree")"

provider="${GRINDSTONE_SENIOR_PROVIDER:-openai-codex}"
model="${GRINDSTONE_PLANNER_MODEL:-gpt-5.6-sol}"
thinking="${GRINDSTONE_PLANNER_EFFORT:-xhigh}"
system_prompt='You are the WILDFLOWS planner. Follow the supplied contract. Emit exactly one PlannerDecision JSON object inside one ```json fenced block. Do not emit a second JSON object.'
out="$log_dir/pi.stdout.log"; err="$log_dir/pi.stderr.log"
limit=()
if [[ -n "$timeout" ]] && command -v timeout >/dev/null 2>&1; then
  limit=(timeout --signal=KILL "$timeout")
elif [[ -n "$timeout" ]] && command -v gtimeout >/dev/null 2>&1; then
  limit=(gtimeout --signal=KILL "$timeout")
fi

set +e
(
  cd "$worktree"
  "${limit[@]}" pi --provider "$provider" --model "$model" --thinking "$thinking" \
    --mode text --print --no-session --append-system-prompt "$system_prompt" \
    < "$prompt" > "$out" 2> "$err"
)
rc=$?
set -e
tail -c 65536 "$err" >&2 || true
if [[ "$rc" -ne 0 ]]; then
  echo "planner-picodex: pi exited $rc (provider=$provider model=$model)" >&2
  grep -hiE 'rate.?limit|429|quota|usage limit|session limit|weekly limit|plan limit|too many requests' \
    "$out" "$err" 2>/dev/null | head -3 >&2 || true
  exit "$rc"
fi

# Noise outside one fence is harmless. The fence body (or an entirely unfenced
# response) must itself be exactly one JSON object; arrays/scalars/extra JSON fail.
python3 - "$out" <<'PY'
import json
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()

def reject_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant {value}")

def decode(candidate: str) -> object:
    return json.loads(candidate, parse_constant=reject_constant)

try:
    value = decode(text.strip())
except (json.JSONDecodeError, ValueError):
    fence = re.search(r"```(?:json)?\s*(.*)```", text, flags=re.IGNORECASE | re.DOTALL)
    try:
        value = decode(fence.group(1).strip()) if fence else decode(text.strip())
    except (json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(f"planner-picodex: malformed decision JSON: {exc}") from exc
if not isinstance(value, dict):
    raise SystemExit("planner-picodex: decision must be one JSON object")
print(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")))
PY
