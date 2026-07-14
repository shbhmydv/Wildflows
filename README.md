# wildflows

**Better dynamic workflows.** Multi-harness, multi-model-ready, long-running,
crash-recoverable, fully journalled — open source.

> The state machine is the hands, the model is the mind.

wildflows is an agent-workflow orchestrator. A planner-model owns *strategy*
(which workflow shape to run, how to verify, when to end); a deterministic core
owns *effects* (git, disk, journal, budgets/rails). Workflows are not a registry
of files — they are **expressions** over seven primitives:

```
do(task, rig, ctx)     one agent, one task -> a result
dispatch(tasks)        parallel do()s
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

Current core: fsynced journal/replay, per-node worktree execution, exact
receipt/run-branch verification, executable `do`, `inplace`, `ask`, `setup`, `seq`,
bounded `dispatch`, and command `loop`. A registered rig now drives the durable
planner/run loop; deadline/max-epoch rails and planner-nudge macro listings are live.
`combine`, a real picodex planner script, and the dashboard remain on the build ladder.
See [`docs/DESIGN.md`](docs/DESIGN.md) and
[`docs/PLANNER-RIG.md`](docs/PLANNER-RIG.md).

## Develop

```
pip install -e '.[dev]'
pytest
mypy
```

`Run` stores state at `.wildflows/runs/<run-id>/` in the target repository: the
journal, verbatim planner decisions, bounded-result artifacts, and disposable worktrees.
Add `.wildflows/runs/` to the target project's ignore rules when needed.

## Topics

`agent workflows` · orchestrator · llm · worktrees · journalling · open source
