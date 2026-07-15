# Frame rig adapters

A v2 frame rig is an agent process that receives a prompt, works in one engine-created
CWD, can call the run's MCP tools, and exits with final text. `rigs.yaml` supports:

- `echo`: deterministic no-tool test rig;
- `shell`: a bounded shell command (useful for test/fake frame binaries);
- `script`: the production prompt-file adapter contract.

The script contract remains:

```text
<script> --worktree DIR --prompt FILE --log-dir DIR \
         --handle-out FILE --timeout SECONDS
```

The engine additionally supplies `WILDFLOWS_MCP_URL`, `WILDFLOWS_RUN_TOKEN`,
`WILDFLOWS_FRAME_ID`, `WILDFLOWS_PI_EXTENSION`, and
`WILDFLOWS_NEXT_CALL_INDEX` in the process environment. Runtime files and the generated
Pi extension are under the run directory, never the gated worktree. The engine owns the
outer process-group timeout, captures both streams, and classifies rate/quota signatures
as `busy`.

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

Both scripts keep prompts out of argv, write their process-group id to `--handle-out`,
set a Git ceiling, return final text on stdout, diagnostics on stderr, and propagate the
transport exit status.

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

```yaml
rigs:
  senior:
    kind: script
    script: rigs/worker-picodex.sh
    log_dir: /tmp/wildflows-logs/senior
    timeout_s: 1800
  local:
    kind: script
    script: rigs/worker-local.sh
    log_dir: /tmp/wildflows-logs/local
    timeout_s: 900
```

Relative script/log paths resolve from the YAML file. Every dispatch rig name must be in
this registry; the registry is the per-run allowlist.
