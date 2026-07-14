# Bundled rig adapters

The scripts in [`rigs/`](../rigs/) implement the `ScriptRig` process contract:

```text
<script> --worktree DIR --prompt FILE --log-dir DIR \
         --handle-out FILE --timeout SECONDS
```

Each adapter reads the prompt file into its child process through **stdin** (never an
argv value), writes its process-group id to `--handle-out`, sets a
`GIT_CEILING_DIRECTORIES` fence, emits only result text on stdout, and returns nonzero
on transport or response failure. `ScriptRig` maps nonzero rate/quota/session text to
`outcome: busy` with its standard patterns. No adapter contains a secret; credentials
are inherited from the environment.

## Adapters

- **`planner-picodex.sh`** runs `pi --mode text --print --no-session` with provider
  `openai-codex`, model `gpt-5.6-sol`, and `xhigh` thinking. The model may surround its
  decision with prose and one JSON fence. The adapter strips that fence, requires one
  JSON object, and prints only compact JSON. Malformed output exits nonzero, so
  `Run` raises retryable `PlannerFailure`. Overrides:
  `GRINDSTONE_SENIOR_PROVIDER`, `GRINDSTONE_PLANNER_MODEL`,
  `GRINDSTONE_PLANNER_EFFORT`.
- **`worker-local.sh`** calls the OpenAI-compatible chat-completions endpoint
  `http://127.0.0.1:8080/v1/chat/completions` with `curl`. Overrides:
  `WILDFLOWS_LOCAL_URL` and `WILDFLOWS_LOCAL_MODEL`. The default loopback endpoint
  needs no credential, so no secret is placed in curl's process arguments.
- **`worker-picodex.sh`** runs a senior agent inside the supplied worktree through
  `pi`; its system prompt requires relative writes and worktree-local commits. Overrides:
  `GRINDSTONE_SENIOR_PROVIDER`, `GRINDSTONE_SENIOR_MODEL`,
  `GRINDSTONE_SENIOR_EFFORT`.

`pi` keeps its own authentication/configuration outside this repository. Relative
`script` and `log_dir` values are resolved from the `rigs.yaml` directory.

## `rigs.yaml`

```yaml
rigs:
  planner:
    kind: script
    script: rigs/planner-picodex.sh
    log_dir: .wildflows-adapter-logs/planner
    timeout_s: 1800
  local:
    kind: script
    script: rigs/worker-local.sh
    log_dir: .wildflows-adapter-logs/local
    timeout_s: 900
  senior:
    kind: script
    script: rigs/worker-picodex.sh
    log_dir: .wildflows-adapter-logs/senior
    timeout_s: 1800
```

Validate without model access:

```bash
bash -n rigs/*.sh
python3 -m pytest -q tests/test_adapters.py
```
