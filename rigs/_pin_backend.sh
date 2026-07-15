# The local llama.cpp stack has two independent GPU backends behind an nginx
# least-connection router. Pi opens one HTTP request per turn, so an idle gap can
# send the next turn to the other GPU and force a full prompt prefill; holding an
# advisory lock for the frame's lifetime pins all of its turns to one backend.
# The open descriptor is process-owned, so exit (including SIGKILL) releases the
# lane automatically, while a third frame queues instead of using the router.

pin_local_backend() {
  local provider_var="$1"
  local user_override="$2"
  local lock_dir="${WILDFLOWS_PIN_LOCK_DIR:-/tmp/llama-servers}"

  if [[ -n "$user_override" ]]; then
    printf -v "$provider_var" '%s' "$user_override"
    return 0
  fi

  mkdir -p "$lock_dir"
  exec {_WILDFLOWS_PIN_FD}> "$lock_dir/pin-8081.lock"
  if flock -n "$_WILDFLOWS_PIN_FD"; then
    printf -v "$provider_var" '%s' local-reviewer-8081
    return 0
  fi

  exec {_WILDFLOWS_PIN_FD}>&-
  exec {_WILDFLOWS_PIN_FD}> "$lock_dir/pin-8082.lock"
  if flock -n "$_WILDFLOWS_PIN_FD"; then
    printf -v "$provider_var" '%s' local-reviewer-8082
    return 0
  fi

  exec {_WILDFLOWS_PIN_FD}>&-
  exec {_WILDFLOWS_PIN_FD}> "$lock_dir/pin-8081.lock"
  flock "$_WILDFLOWS_PIN_FD"
  printf -v "$provider_var" '%s' local-reviewer-8081
}
