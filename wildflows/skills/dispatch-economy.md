# Dispatch economy — Route bounded work by determinacy and spend review once

Use delegation only when it buys more than doing the work in this frame. The system schedules capacity; your job is to choose the smallest useful task and rig.

- Choosing a tier is part of writing each task. Route by **specification determinacy**, not by subject: default to the cheapest rig that can execute the specification mechanically. Use a stronger rig for open requirements, taste, architecture, cross-component tradeoffs, or the same task after a cheaper attempt failed.
- `kinds` may label the nature of each task (suggested: `implement`, `review`, `research`, `artifact`), but never route it. Pass `rig` when deliberately changing tier; omission or a null per-task entry inherits this frame's rig.
- Budget at most one commissioned review per component, normally on the cheap worker rig. Deterministic gates are the primary quality signal. Never audit an audit, and do not commission a second review because another orchestration layer exists.
- Price a delegation by the whole tree it can create, not only its direct fan-out. An audit or verification task gets no subtree: if a verification child needs to delegate, its task was scoped wrong. State the intended total frame count for the fan-out in this dispatching frame's own reasoning, and stop delegating when that count is spent.
- Treat a returned child report and integrated commits as authoritative. Inspect only the evidence needed to integrate or resolve a concrete contradiction; do not reflexively redo the child's work.
- Keep work in-frame when it is smaller than roughly one file plus its focused tests. Prefer one coherent, well-scoped child over many parallel micro-tasks. Bound both dispatch depth and fan-out to the minimum that closes the task.
- All delegation goes through `wildflows_dispatch`. Out-of-band workers are not journalled, integrated, replayable, or covered by the engine's lifecycle controls.

## When a child fails

The failed result names its salvage branch, head, and bounded diffstat. Choose one disposition; do not discard useful committed work by reflex.

- **Retry** a transient or plausibly one-more-go failure with `retry_frame` alone. The same child branch relaunches with its prior commits and bounded earlier-attempt evidence.
- **Inline** when only a sliver remains: merge the salvage branch into this worktree yourself and finish here.
- **Escalate** by sending the same task to a stronger rig with the concrete failure evidence and salvage branch attached.
- **Ask/park** when only the human can unblock progress: use `wildflows_ask` rather than spinning or claiming success.
