# Frame rig adapters

A v2 frame rig is an agent process that receives a prompt, works in one engine-created
CWD, can call the run's MCP tools, and exits with final text. Every entry may set an
optional, nonblank, single-line `description`; when that rig is currently dispatchable,
the engine shows it beside the registry key in frame resource preambles. An entry
without `description` renders name-only. `rigs.yaml` supports:

- `echo`: deterministic no-tool test rig with no kind-specific fields;
- `shell`: a bounded shell command requiring `template` and positive `timeout_s`;
- `script`: the production prompt-file adapter contract, requiring `script` and
  `log_dir`, with optional positive `timeout_s` (default 900), `env`, and
  `busy_patterns`.

The script contract remains:

```text
<script> --worktree DIR --prompt FILE --log-dir DIR \
         --handle-out FILE --timeout SECONDS
```

The engine additionally supplies `WILDFLOWS_MCP_URL`, `WILDFLOWS_RUN_TOKEN`,
`WILDFLOWS_FRAME_ID`, `WILDFLOWS_PI_EXTENSION`, and
`WILDFLOWS_NEXT_CALL_INDEX` in the process environment. Runtime files and the generated
Pi extension are under the run directory, never the gated worktree. The engine launches every adapter as a session leader, owns its durable runtime handle,
captures both streams, and classifies rate/quota signatures as `busy`. Shutdown sends
SIGTERM to the recorded process group and every member of its session, waits a bounded
grace period, then SIGKILLs survivors.

## Bundled adapters

- **`worker-picodex.sh`** is the reference resident frame. It reads the prompt through
  stdin, loads the generated extension with `pi -e`, and tells Pi to commit before tool
  calls or exit. Provider/model/effort overrides are
  `GRINDSTONE_SENIOR_PROVIDER`, `GRINDSTONE_SENIOR_MODEL`, and
  `GRINDSTONE_SENIOR_EFFORT`.
- **`worker-local.sh`** runs the same resident Pi contract while flock-pinning the
  frame to one local GPU backend. It defaults to model `qwen-3-6-27b-dense` at
  `medium` effort; `GRINDSTONE_SENIOR_PROVIDER` is an explicit operator pin, and
  the model/effort overrides match `worker-picodex.sh`.

Both scripts keep prompts out of argv and preserve the `--handle-out` contract. Handles
are now JSON records containing the adapter PID, process-group ID, and session ID; the
engine reader also accepts legacy one-integer PGID handles. The scripts set a Git ceiling,
return final text on stdout, diagnostics on stderr, and propagate the transport exit
status.

## Local GPU pool

The local llama.cpp stack exposes two independent GPU backends behind an nginx
least-connection router. Pi makes one HTTP request per turn, so the router sees no
active connections between turns and alternates backends, making a multi-turn frame
migrate GPUs and re-prefill its full prompt.

Use a pooled `local` rig backed by `worker-local.sh` for normal local dispatches: it
tries `local-reviewer-8081`, then `local-reviewer-8082`, and waits for 8081 when both
are occupied, keeping its lock for Pi's entire lifetime. A third parallel frame queues
rather than falling back to nginx. `WILDFLOWS_PIN_LOCK_DIR` overrides the lock directory
(default `/tmp/llama-servers`) for isolated tests.

Keep explicit `local-a`/`local-b` rigs only when an operator needs a fixed lane for
benchmarking, diagnosis, or reservation. Configure those names with
`GRINDSTONE_SENIOR_PROVIDER: local-reviewer-8081` or `local-reviewer-8082`; a non-empty
provider override bypasses lock creation and acquisition entirely.

## Example YAML

The top level requires `rigs` and may also set one nonblank, single-line `notify`
command. `--notify` overrides that YAML value. After each newly journalled owner ask,
the engine attempts to launch the command detached from the repository root, appending
the question, frame id, and run id as arguments and setting `WILDFLOWS_QUESTION`,
`WILDFLOWS_FRAME_ID`, and `WILDFLOWS_RUN_ID`. Exact ask replay does not notify again;
spawn failure and notifier exit status cannot affect the run.

```yaml
# notify: /path/to/owner-notify
rigs:
  senior:
    kind: script
    description: deep architecture and review lane
    script: rigs/worker-picodex.sh
    log_dir: /tmp/wildflows-logs/senior
    timeout_s: 1800
  local:
    kind: script
    description: pooled dual-GPU Qwen lane for concretely-specced junior work
    script: rigs/worker-local.sh
    log_dir: /tmp/wildflows-logs/local
    timeout_s: 900
```

Relative script/log paths resolve from the YAML file. Every dispatch rig name must be in
this registry; the registry is the per-run allowlist. Rig names are the keys (`senior`,
`local` above), not adapter script filenames such as `worker-local.sh`.
