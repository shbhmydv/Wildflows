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
- **Core-enforced rails direction** — loop caps are live now; budget and deadline
  rails land with the later composition/worktree steps.
- **Multi-harness rigs** — `claude -p`, `pi`, local Qwen, `codex` all plug in
  behind one prompt-in / files-out seam.

The design's eventual default macro is a **BUILD** run followed by a spec-unbound
**AUDIT** run (an expert panel judges the artifact, not the tasks); that planner
macro is not part of the serial PoC yet.

## Status

Serial proof-of-concept: fsynced journal/replay, per-node worktree execution, exact
receipt/run-branch verification, executable `do`, `inplace`, `seq`, serial `dispatch`,
and command `loop`, plus `EchoRig`, `ShellRig`, and `ScriptRig`. Parallel dispatch,
general rails, planner macros, and the dashboard remain on the build ladder. See
[`docs/DESIGN.md`](docs/DESIGN.md).

## Develop

```
pip install -e '.[dev]'
pytest
mypy
```

Run state (journal, artifacts, and disposable worktrees) must live outside the target
repository worktree. Target-local `.wildflows/` state remains a later authority step.

## Topics

`agent workflows` · orchestrator · llm · worktrees · journalling · open source
