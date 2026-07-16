# Dispatch economy — Route bounded work by determinacy and spend review once

Use delegation only when it buys more than doing the work in this frame. The system schedules capacity; your job is to choose the smallest useful task and rig.

- Route by **specification determinacy**, not by subject. A closed specification with a mechanical acceptance check belongs on the cheap worker rig. Open requirements, taste, architecture, and cross-component tradeoffs belong on a senior rig once; that senior should close the specification and route bounded execution downward.
- When the repository configures `kinds:` defaults, attach a free-text kind to every task (suggested: `implement`, `review`, `research`, `artifact`) and let that mapping select the rig. An explicit rig remains appropriate when the task is an exception.
- Budget at most one commissioned review per component, normally on the cheap worker rig. Deterministic gates are the primary quality signal. Never audit an audit, and do not commission a second review because another orchestration layer exists.
- Treat a returned child report and integrated commits as authoritative. Inspect only the evidence needed to integrate or resolve a concrete contradiction; do not reflexively redo the child's work.
- Keep work in-frame when it is smaller than roughly one file plus its focused tests. Prefer one coherent, well-scoped child over many parallel micro-tasks. Bound both dispatch depth and fan-out to the minimum that closes the task.
- All delegation goes through `wildflows_dispatch`. Out-of-band workers are not journalled, integrated, replayable, or covered by the engine's lifecycle controls.
