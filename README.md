# wildflows

**Better dynamic workflows.** Multi-harness, multi-model, long-running,
worktree-isolated, fully journalled — open source.

> The state machine is the hands, the model is the mind.

wildflows is an agent-workflow orchestrator. A planner-model owns *strategy*
(which workflow shape to run, how to verify, when to end); a deterministic core
owns *effects* (git, disk, journal, budgets/rails). Workflows are not a registry
of files — they are **expressions** over seven primitives:

```
do(task, rig, ctx)     one agent, one task, in a worktree -> a result
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

- **Worktree-mediated effects** — agents never touch integration git; the core
  applies and commits. Crashes/OOMs/SIGTERMs lose zero accepted work.
- **One journal, one event vocabulary** — resume = replay the event log against
  the expression tree. No per-shape resume code.
- **Planner-declared, core-enforced rails** — budget / deadline / iteration caps.
- **Multi-harness rigs** — `claude -p`, `pi`, local Qwen, `codex` all plug in
  behind one prompt-in / files-out seam.

Runs default to a **BUILD** run followed by a spec-unbound **AUDIT** run (an
expert panel judges the artifact, not the tasks).

## Status

Proof-of-concept (ladder step 1): the expression data model, the full journal
event vocabulary, and an engine that executes `do` / `inplace` trees with two
rigs (`EchoRig`, `ShellRig`). See [`docs/DESIGN.md`](docs/DESIGN.md) for the
design-of-record and the build ladder.

## Develop

```
pip install -e '.[dev]'
pytest
mypy
```

Target-repo run state lives under `.wildflows/`.

## Topics

`agent workflows` · orchestrator · llm · worktrees · journalling · open source
