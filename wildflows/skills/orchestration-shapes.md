# Orchestration shapes — Compose serial work, independent fan-out, bounded loops, and reviews

`wildflows_dispatch` blocks until its children finish and their commits are integrated. A multi-task call is serial by default; use `parallel: true` only for independent work. Several dispatch calls from the same frame compose larger shapes.

## Serial pipeline (default)

Choose serial when a later task depends on an earlier result. Each task in one call runs after the previous child has integrated, so it sees the updated branch.

**Example:** dispatch `['implement the migration and run its focused tests', 'verify the migrated behavior and repair only demonstrated failures']` without `parallel`. Cost is the sum of both tasks, but the second task starts with current evidence and avoids speculative duplicate work.

## Parallel fan-out

Choose parallel only when tasks have disjoint files or modules, no ordering dependency, and a cheap join. State those ownership boundaries in each task.

**Example:** dispatch three independent adapter implementations with `parallel: true`, one adapter directory per task; after the call returns, run the shared gate in this frame. Parallelism reduces elapsed time but pays for every branch at once and raises integration-conflict risk.

## Bounded loop

Choose a loop when the next task cannot be scoped until a result or gate is inspected. Dispatch once, inspect the integrated result, and redispatch a focused correction with exact failure evidence. Set the retry budget before starting.

**Example:** with a retry budget of two, dispatch one implementation, run the acceptance gate, and only on failure dispatch `fix this failure: <command, exit code, relevant output>`; stop after a pass or two corrections. Loops pay only for observed defects, but an unbounded loop can spend indefinitely.

When the budget exhausts and the gate still fails, choose a failed-child disposition deliberately. Never silently extend the loop. Finish inline if the residual is small; send the same task to a stronger rig with the concrete failure evidence and salvage branch if it was under-tiered; otherwise fail honestly upward with the concrete evidence. A truthful failure gives the parent something actionable; fake success does not.

## Composite shape

Compose shapes with sequential calls: finish one call, inspect its combined branch, then make the next call.

**Example:** first dispatch three disjoint implementations with `parallel: true`; after all integrate, make one serial dispatch for a single review of the combined diff. This is `serial(parallel × 3 implement → review)`: it buys one join-level review rather than three local reviews plus another audit.

Prefer the shape with the fewest paid frames that preserves necessary dependency order. Serial pipelines minimize conflict and speculation; parallel fan-out buys latency with wider spend; bounded loops buy adaptation; composites should add a stage only when it has distinct marginal value.
