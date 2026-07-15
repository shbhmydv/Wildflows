#!/usr/bin/env bash
# ScriptRig adapter for a flock-pinned local pi worker in its disposable worktree.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_pin_backend.sh
source "$script_dir/_pin_backend.sh"

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

provider="${GRINDSTONE_SENIOR_PROVIDER:-}"
model="${GRINDSTONE_SENIOR_MODEL:-qwen-3-6-27b-dense}"
thinking="${GRINDSTONE_SENIOR_EFFORT:-medium}"
extension="${WILDFLOWS_PI_EXTENSION:-}"
[[ -n "$extension" && -f "$extension" ]] || {
  echo "worker-local: WILDFLOWS_PI_EXTENSION is missing or unreadable" >&2
  exit 2
}
pin_local_backend provider "$provider"
system_prompt='You are a WILDFLOWS frame. Work only inside this worktree (your CWD), use relative paths, commit useful changes before engine tool calls or exit, and return a concise final report.'
out="$log_dir/pi.stdout.log"; err="$log_dir/pi.stderr.log"
limit=()
if [[ -n "$timeout" ]] && command -v timeout >/dev/null 2>&1; then
  limit=(timeout --signal=KILL "$timeout")
elif [[ -n "$timeout" ]] && command -v gtimeout >/dev/null 2>&1; then
  limit=(gtimeout --signal=KILL "$timeout")
fi
pi_args=(
  -e "$extension" --provider "$provider" --model "$model" --thinking "$thinking"
  --mode text --print --no-session --append-system-prompt "$system_prompt"
)

cd "$worktree"
set +e
if [[ -n "${_WILDFLOWS_PIN_FD:-}" ]]; then
  # The wrapper retains its lock while foreground pi runs; pi does not inherit it.
  "${limit[@]}" pi "${pi_args[@]}" < "$prompt" > "$out" 2> "$err" {_WILDFLOWS_PIN_FD}>&-
else
  "${limit[@]}" pi "${pi_args[@]}" < "$prompt" > "$out" 2> "$err"
fi
rc=$?
set -e
tail -c 65536 "$err" >&2 || true
tail -c 262144 "$out" || true
if [[ "$rc" -ne 0 ]]; then
  echo "worker-local: pi exited $rc (provider=$provider model=$model)" >&2
  grep -hiE 'rate.?limit|429|quota|usage limit|session limit|weekly limit|plan limit|too many requests' \
    "$out" "$err" 2>/dev/null | head -3 >&2 || true
fi
exit "$rc"
