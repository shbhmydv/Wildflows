# Planner rig contract (v1)

The planner is not a privileged engine plug-in. `Run` resolves one named rig from the
same `RigRegistry` used by `do`, builds a prompt, and calls:

```python
planner.run(prompt, repository_root) -> Result
```

A non-`ok` result or malformed output raises retryable `PlannerFailure`. The bundled
[`planner-picodex.sh`](../rigs/planner-picodex.sh) is the real `pi` adapter; model calls
remain operator-run, while tests put a fake `pi` on `PATH`.

## Script process contract

A planner configured as the existing `ScriptRig` is launched with its normal
prompt-file contract:

```text
<script> --worktree <repository-root> --prompt <prompt-file> \
         --log-dir <dispatch-log-dir> --handle-out <handle-file> \
         --timeout <seconds>
```

The script must print exactly one UTF-8 JSON decision to stdout on success and exit 0.
Fence/noise cleanup belongs inside an adapter, never the core. The bundled picodex
adapter asks the model for one fenced object, strips surrounding noise, validates that
object, and prints compact unfenced JSON. Invalid extraction exits nonzero. The planner
should not mutate the repository; effects belong in its emitted expression.

Every invocation's `Result.text` is atomically retained under
`.wildflows/runs/<run-id>/decisions/` before JSON parsing. On success that is the exact
JSON stdout. On a nonzero ScriptRig exit it is the selected error surface; raw adapter
transport streams remain in that dispatch's log directory. The prompt names the run
directory so a capable rig may inspect full result artifacts there.

## Decision JSON

```json
{
  "expression": {"kind": "do", "task": "...", "rig": {"name": "senior"}},
  "rails": {
    "deadline_s": 1800,
    "max_epochs": 4,
    "budget_notes": "No token accounting is enforced in M4."
  },
  "rationale": "A focused senior pass is the shortest path.",
  "end": false,
  "final_summary": null
}
```

`expression` is the existing expression JSON. It must be non-null while `end` is false.
An ending decision uses `expression: null`, `end: true`, and a non-empty
`final_summary`. Unknown fields are rejected. Expression Pydantic parsing and the normal
admission pass happen before `boundary(opened)`.

The first expression decision declares deadline/max-epoch rails. Later decisions may
update them; `deadline_s` may only decrease. Deadline is measured from durable run
creation. Rails refuse to start additional core work rather than interrupting in-flight
work. A hit raises `RailStop` while preserving an open expression for identical resume.

## Planner prompt

The prompt contains:

- the complete durable job markdown;
- a deterministic digest of the immediately prior epoch's effective per-node results;
- macro names, descriptions, and source paths;
- current rails plus elapsed time; and
- the full run-artifact directory.

Result previews are capped per node and globally, paths/node count are capped with
truncation markers, and each ordinary result links its run-relative full JSON artifact.
Only effective journal events are digested, so fallback-invalidated tails do not leak
into planner context.

Macros are JSON data in `wildflows/macros/` or `<run-dir>/../macros/`. They are nudges,
not a core expansion mechanism: the prompt lists them and the planner emits an already
expanded expression. The built-ins `senior-loop` and `swarm-judge` demonstrate the data
shape.

## Ask resume API

An unanswered `ask` leaves the epoch open and raises `AwaitingOwner`. Supply an answer
without replanning that epoch:

```python
run.resume(answer="blue", answer_node="n0.0")
run.resume(answer_file=Path("owner-answer.txt"), answer_node="n0.0")
```

The durable `answered` event projects directly to that Ask node's `Result`; downstream
node context reads the owner text exactly as it reads any other node result.

A setup with `idempotent: false` that was dispatched but did not succeed is surfaced as
`SetupResumeRequired`, never silently repeated. After inspecting the host, the owner can
make the retry explicit with `run.resume(retry_setups=True)`.
