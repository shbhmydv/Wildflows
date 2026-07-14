# wildflows

**Better dynamic workflows.** Multi-harness, multi-model-ready, long-running,
crash-recoverable, fully journalled — open source.

> The state machine is the hands, the model is the mind.

wildflows is an agent-workflow orchestrator. A planner-model owns *strategy*
(which workflow shape to run, how to verify, when to end); a deterministic core
owns *effects* (git, disk, journal, budgets/rails). Workflows are not a registry
of files — they are **expressions** over eight primitives:

```
do(task, rig, ctx)     one agent, one task -> a result
dispatch(tasks)        parallel do()s
seq(children)           strict ordered composition
combine(results, task) a do() whose input is other results
loop(expr, until, cap) repeat a sub-expression until a condition or cap
inplace(edit)          the planner's own hands: a small fix, core commits it
ask(owner)             park until the owner answers a genuine decision
setup(cmd)             a journaled host mutation (npm ci, boot a dev server)
```

Named shapes (swarm, battle, senior→junior loop) are just **macros** — saved
expressions that act as nudges to the planner.

## Why it's different

Multi-CLI orchestrators exist. wildflows is built for **survivability**:

- **Core-accounted effects** — every `do`, `inplace`, and predicate gets a fresh,
  never-reused detached worktree. Successful commit ranges fast-forward onto the run
  branch; failed/interrupted worktrees are abandoned, so there is nothing to undo.
- **One journal, one event vocabulary** — resume = replay the event log against
  the expression tree. No per-shape resume code.
- **Core-enforced rails** — planner-declared run deadlines, maximum epochs, and
  expression loop caps stop work at deterministic start boundaries.
- **Multi-harness rigs** — `claude -p`, `pi`, local Qwen, `codex` all plug in
  behind one prompt-in / files-out seam.

The design's eventual default macro is a **BUILD** run followed by a spec-unbound
**AUDIT** run (an expert panel judges the artifact, not the tasks); that planner
macro is not part of the core yet.

## Status

Current core: v1 fsynced journal/replay, per-node worktree execution, exact
receipt/run-branch verification, all eight expression kinds (command predicates for
`loop`), bounded `dispatch`, and planner/run rails. Bundled adapters cover picodex
planner/senior roles and the local OpenAI-compatible worker. The dashboard remains on
the build ladder. See [`docs/DESIGN.md`](docs/DESIGN.md),
[`docs/PLANNER-RIG.md`](docs/PLANNER-RIG.md), and [`docs/RIGS.md`](docs/RIGS.md).

## Develop

```
pip install -e '.[dev]'
python3 -m pytest -q
python3 -m mypy --strict wildflows tests
```

`Run` stores state at `.wildflows/runs/<run-id>/` in the target repository: the
journal, verbatim planner decisions, bounded-result artifacts, and disposable worktrees.
Add `.wildflows/runs/` to the target project's ignore rules when needed.

Run the live adapter smoke from the checkout (see the example README for prerequisites):

```bash
python3 -m wildflows run examples/toy-run/job.md --repo <target>
```

Resume with `python3 -m wildflows resume ... --run-id <id>`; add `--answer TEXT`
for a parked owner question.

## Topics

`agent workflows` · orchestrator · llm · worktrees · journalling · open source
