# Frame rig adapters

A v2 frame rig is an agent process that receives a prompt, works in one engine-created
CWD, can call the run's MCP tools, and exits with final text. Every entry may set an
optional, nonblank, single-line `description`; when that rig is currently dispatchable,
the engine shows it beside the registry key in frame resource preambles. An entry
without `description` renders name-only. Every rig may also set a positive integer
`slots`; omission preserves unlimited active execution. Optional positive
`gate_timeout_s` bounds only gates called by that rig; omission leaves gate execution
unbounded by Wildflows. `rigs.yaml` supports:

- `echo`: deterministic no-tool test rig with no kind-specific fields;
- `shell`: a bounded shell command requiring `template` and positive `timeout_s`;
- `script`: the production prompt-file adapter contract, requiring `script` and
  `log_dir`, with optional positive `timeout_s` (default 900), `env`, and
  `busy_patterns`.

`timeout_s` is the per-frame self-time budget. It advances only while the frame is
thinking. Dispatch and ask park the frame; gate execution pauses only its self-time
clock while retaining its active slot lease because the resident worker remains alive.
Gate waits are bounded only when that rig sets `gate_timeout_s`; timeout returns exit 124
and a clear stderr marker. The engine reaps a worker that exhausts self-time. Adapter
commands receive a `3 × timeout_s` crash backstop rather than the authoritative budget;
run/subtree backstops also remain independent of gate configuration.

The script contract remains:

```text
<script> --worktree DIR --prompt FILE --log-dir DIR \
         --handle-out FILE --timeout BACKSTOP_SECONDS
```

The engine additionally supplies `WILDFLOWS_MCP_URL`, `WILDFLOWS_RUN_TOKEN`,
`WILDFLOWS_FRAME_ID`, `WILDFLOWS_PI_EXTENSION`, and
`WILDFLOWS_NEXT_CALL_INDEX` in the process environment. A finite-slot lease also supplies
`WILDFLOWS_SLOT` (zero-based) and the bundled local-stack
`WILDFLOWS_PROVIDER_OVERRIDE` for that lane. Runtime files and the generated
Pi extension are under the run directory, never the gated worktree. The engine launches
every adapter as a session leader, owns its durable runtime handle, captures both streams,
and classifies rate/quota signatures as `busy`. Shutdown sends
SIGTERM to the recorded process group and every member of its session, waits a bounded
grace period, then SIGKILLs survivors.

## Bundled adapters

- **`worker-picodex.sh`** is the reference resident frame. It reads the prompt through
  stdin, loads the generated extension with `pi -e`, and tells Pi to commit before tool
  calls or exit. Provider/model/effort overrides are
  `GRINDSTONE_SENIOR_PROVIDER`, `GRINDSTONE_SENIOR_MODEL`, and
  `GRINDSTONE_SENIOR_EFFORT`.
- **`worker-local.sh`** runs the same resident Pi contract on the backend assigned by
  the engine's active slot lease. It defaults to model `qwen-3-6-27b-dense` at
  `medium` effort; `GRINDSTONE_SENIOR_PROVIDER` is an explicit operator override of
  the assigned lane, and the model/effort overrides match `worker-picodex.sh`.

Both scripts keep prompts out of argv and preserve the `--handle-out` contract. Handles
are now JSON records containing the adapter PID, process-group ID, session ID, and (when
written by the Linux engine) process start-time generation; the engine reader also accepts
legacy one-integer PGID handles. The scripts set a Git ceiling,
return final text on stdout, diagnostics on stderr, and propagate the transport exit
status.

## Local GPU pool

The local llama.cpp stack exposes two independent GPU backends behind an nginx
least-connection router. Pi makes one HTTP request per turn, so the router sees no
active connections between turns and alternates backends, making a multi-turn frame
migrate GPUs and re-prefill its full prompt.

Use one pooled `local` rig backed by `worker-local.sh` with `slots: 2`. The engine maps
slot 0/1 to `local-reviewer-8081`/`local-reviewer-8082`, prefers a frame's prior lane
(with a stable frame-id hash for its first lease), and queues excess active work in the
journal. A frame releases its lane while parked on dispatch or ask and reacquires before
the blocking response is delivered, so parked ancestors cannot deadlock a nested third
frame. The former advisory-flock helper and its blocking fallback no longer exist.

Keep explicit `local-a`/`local-b` rigs only for operator diagnosis or reservation. An
explicit `GRINDSTONE_SENIOR_PROVIDER` still takes precedence over the engine lane.

## Fresh-worktree provisioning

The optional top-level `worktree` section belongs to the target repository, not a rig:

- `setup` is one shell command run in each newly created frame worktree after checkout
  and before its adapter launches. A nonzero exit terminalizes that frame launch, records
  bounded stdout/stderr in the failure, and removes the worktree cleanly.
- `link` is a list of repository-relative paths. Each existing source in the primary
  checkout is symlinked at the same path in every new frame worktree; a missing source is
  skipped with a journalled warning. Links share mutable state, which suits caches and
  dependency directories but not frame-owned source or build outputs.

Each configured mechanism records `worktree_provisioned` with its worktree, duration,
outcome, linked paths/warnings, and bounded setup output. Provisioning is once per fresh
worktree path. Replay returns completed calls without provisioning again; a resumed
attempt that must create a replacement worktree provisions that new checkout once.
`setup` runs with no Wildflows hard timeout, so owners should make it deterministic and
supply any repository-appropriate bound inside the command when needed. Link paths may
not be absolute, escape with `..`, name `.git`, or repeat.

## Example YAML

The top level requires `rigs` and may also set one nonblank, single-line `notify`
command, a `kinds` mapping from free-text task kind to a declared rig, and the repository
`worktree` section above. A dispatch's
explicit `rig` wins; otherwise every task needs a kind with a configured default. No
kind mapping is supplied by Wildflows. `--notify` overrides the YAML notify value. After
each newly journalled owner ask, the engine attempts to launch the command detached from
the repository root, appending
the question, frame id, and run id as arguments and setting `WILDFLOWS_QUESTION`,
`WILDFLOWS_FRAME_ID`, and `WILDFLOWS_RUN_ID`. Exact ask replay does not notify again;
spawn failure and notifier exit status cannot affect the run.

```yaml
# notify: /path/to/owner-notify
worktree:
  setup: python3 -m project_bootstrap --worktree
  link:
    - .cache/dependencies
kinds:
  implement: local
  review: local
rigs:
  senior:
    kind: script
    description: deep architecture and review lane
    script: rigs/worker-picodex.sh
    log_dir: /tmp/wildflows-logs/senior
    timeout_s: 1800
    gate_timeout_s: 7200
  local:
    kind: script
    description: pooled dual-GPU Qwen lane for concretely-specced junior work
    script: rigs/worker-local.sh
    log_dir: /tmp/wildflows-logs/local
    timeout_s: 900
    slots: 2
```

Relative script/log paths resolve from the YAML file. Every dispatch rig name must be in
this registry; the registry is the per-run allowlist. Rig names are the keys (`senior`,
`local` above), not adapter script filenames such as `worker-local.sh`.
