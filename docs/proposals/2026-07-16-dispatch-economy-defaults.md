# Proposal: default dispatch economy, orchestration shapes, and kind hints

Status: ACCEPTED 2026-07-16, implemented in this change.

## Problem

Dogfood runs (U4ME and a React Native app target) show the default orchestration is wasteful in
three recurring ways:

1. **Recursive senior explosion** — 1 senior dispatches 3 seniors dispatches 9,
   with no marginal-value check; mechanical work lands on expensive rigs.
2. **Audit cascades** — each layer audits its children, then the layer above
   re-audits the audited chunks; review dominates spend without improving the
   product past the first pass.
3. **Parallel monoculture** — dispatches are almost always `parallel` fan-outs.
   Serial pipelines, loops (dispatch → inspect → redispatch), and composite
   shapes (serial(parallel×3 → review)) effectively never occur.

Both target repos fixed 1–2 locally with a `.wildflows/skills/dispatch-economy.md`
routing skill. Nothing fixes 3 anywhere yet.

## Root-cause findings (verified against the code, 2026-07-16)

- **The tool spec hides the shape vocabulary.** The `dispatch` MCP description
  is a single line ("Dispatch one or more child frame tasks."). The schema has
  `parallel: {default: false}` — *serial is already the default* for a
  multi-task dispatch — and composite shapes fall out of simply making several
  sequential `dispatch` calls from one resident frame (the call blocks; results
  come back; the frame can react, loop, or re-scope). None of this is stated,
  so the model falls back on its generic agentic prior: one big parallel
  fan-out. The capability exists; it is unadvertised.
- **The default skills teach tiering, not shapes or economy.** `skill-selection`
  and `long` describe junior/senior roles but contain no routing-by-cost
  doctrine, no audit budget, and no orchestration shapes. Worse, `long.md`
  instructs seniors to "delegate modules through agent-harness subagents" —
  exactly the out-of-band, unjournalled delegation the U4ME skill had to ban,
  and the same escaped-PGID subagents implicated in the stop-leak issue.
- **No dispatch-kind vocabulary.** The picodex rig already infers
  worker-vs-critic by sniffing the `--log-dir` path (`<slug>-worker/` vs
  `<slug>-critic/`) — an implicit, load-bearing contract with no first-class
  field behind it.

## Proposal

Everything is a hint or a skill; the engine enforces nothing new. Per owner
direction: an `implement` task may still research first or lightly review its
own children — kinds must not become straitjackets.

### A. Ship a generalized dispatch-economy default skill

Port U4ME's `dispatch-economy.md` into `wildflows/skills/`, de-specialized:

- Route by **spec determinacy**, not domain: closed spec + mechanical
  acceptance check → cheap worker rig; open-specced/taste/architecture →
  senior, once, whose job is to close the spec and fan bounded work down.
- **Audit budget**: at most one commissioned review per component, on the cheap
  rig; deterministic gates are the primary quality signal; never audit an
  audit; trust returned children (their reports are authoritative).
- **Depth/width discipline**: prefer in-frame work below ~one-file-plus-tests;
  bounded dispatch depth; one well-scoped task over parallel micro-tasks.
- **No out-of-band subagents**: all delegation via `dispatch` so it is
  journalled, integrated, replayable.

### B. Ship an orchestration-shapes default skill (fixes the parallel monoculture)

New `wildflows/skills/orchestration-shapes.md` that makes the existing
capabilities legible, with one worked example each:

- **Serial pipeline** (the default!): multi-task dispatch runs tasks in order,
  each seeing the previous task's integrated result — use for
  implement → review, migrate → verify.
- **Parallel fan-out**: `parallel: true` only when tasks are independent
  (disjoint files/modules) and the join is cheap.
- **Loop**: dispatch, inspect the returned result/gate, redispatch a fix-up
  with the failure evidence — bounded by an explicit retry budget.
- **Composite**: sequential dispatch calls compose shapes —
  e.g. serial( parallel×3 implement → single review of the combined diff ).
- When to pick which, and the cost model for each.

### C. Fatten the `dispatch` tool description (spec, not policy)

Three or four sentences in `mcp.py`: serial-by-default semantics of the task
list, what `parallel` changes, that the call blocks and returns integrated
results, and that sequential calls from one frame are the composition
mechanism. This is surfacing existing semantics, not adding behavior.

### D. `kind` as an optional free-text hint on dispatch

- Optional `kind` field (suggested vocabulary: `implement`, `review`,
  `research`, `artifact` — but free text, unvalidated).
- Journalled on the dispatch event (dashboard + replay can show division of
  labour); `rigs.yaml` may map kind → default rig/model so skills can say
  "reviews go to the cheap rig" without naming rigs; the log-dir path contract
  the picodex rig sniffs gets `kind` as its explicit, durable source.
- Explicitly NOT enforced: no engine rejection of "second review", no
  hardcoded do → implement+review splitting. Default skills express those
  policies; repos override by dropping their own `.wildflows/skills/`.

### E. Align existing defaults (owner-decided 2026-07-16)

- `long.md`: delegation goes through the rig machinery (dispatch), not
  agent-harness subagents. Rationale (owner): dispatch allows senior
  sub-workers, more freedom, better-managed workers — and it is journalled,
  replayable, and covered by the reap path. Subagents remain acceptable only
  for quick in-frame legwork whose loss on relaunch is painless.
- Skills say NOTHING about slots or concurrency. Machine capacity is config,
  not doctrine: per-rig `slots:` in rigs.yaml (see F). "The system schedules;
  you just dispatch."
- `skill-selection.md`: reference the economy and shapes skills as the default
  bundle for any dispatching frame.

### F. Engine-side scheduling + self-time timeouts (owner-raised 2026-07-16)

Two engine changes that are resource facts, not orchestration policy:

- **Per-rig `slots:` in rigs.yaml, leased for ACTIVE periods only
  (owner-refined 2026-07-16).** A slot is not "a live frame" — it is "a frame
  actively running". The engine acquires a lease before launching a frame AND
  before resuming a parent whose blocking call just returned; it releases the
  lease the moment a frame parks on an engine call (dispatch/ask) or exits.
  Excess work waits at the ENGINE for a lease — journalled/dashboard-visible
  "queued for slot", self-time clock paused — never inside llama-server where
  the wait is invisible and burns the rig timer. Consequences: parked
  ancestors hold nothing, so any dispatch depth works on 2 slots; the instant
  a worker parks, the freed slot goes to the next waiter.
- **Slot→backend affinity replaces flock pinning.** On the local rig, slots
  map 1:1 to GPU backends, so the engine's slot assignment IS the affinity
  mechanism: prefer the frame's previous backend (stable frame-id hash) when
  it is among the free slots, otherwise take any free slot (a cold prefill
  costs seconds; enforced idleness costs minutes). `_pin_backend.sh`'s
  advisory flocks — including the depth-3 deadlock in its blocking third
  branch (see open question 6) — are deleted outright once this lands.
- **Self-time timeout accounting.** A frame's timeout budget ticks only while
  the frame is actually running — it pauses for the full duration of any
  blocking engine call (dispatch/gate/ask), which subsumes the child
  queue-wait case. A parked caller holds no inference slot, so charging it
  wall-clock for its subtree's execution (as the rig `timeout --signal=KILL`
  wrapper does today) makes a senior's survival depend on the shape of the
  work beneath it. Consequence: the ENGINE owns timeout enforcement (it
  serves the blocking calls, so it knows every park/resume instant and can
  kill via the reap path on budget exhaustion); the rig-level `--timeout`
  is retained only as a generous crash backstop. Decided (2026-07-16): gate
  execution charges the caller's clock — it is the frame's own work in its
  own worktree, and gates are fast so the charge is marginal either way;
  dispatch and ask do not charge.

## Non-goals

- Engine-enforced orchestration shapes or audit quotas.
- A closed kind taxonomy or per-kind engine behavior.
- Changing dispatch/gate/ask call semantics or the journal format (the `kind`
  field is additive and optional, like `caller_head` in ledger 156).

## Open questions for the owner

1. Should the economy + shapes skills be auto-assigned to every dispatching
   frame (root gets them implicitly), or stay opt-in via `skills:` lists?
2. Does `kind` belong on the dispatch call (per task) or per-task-string
   markup? Per-task field parallel to `skills` seems cleanest.
3. rigs.yaml kind→rig mapping: default it (worker rigs for
   implement/review/research) or leave unmapped until a repo opts in?
4. ~~Rig concurrency as an AdmissionPolicy cap?~~ Superseded by F: per-rig
   `slots:` in rigs.yaml with an engine-side queue. Admission rails stay
   structural (depth/breadth/subtree); scheduling is the engine's job.
5. ~~Keep a subtree wall-clock cap alongside self-time accounting?~~ Resolved by
   Hand-50: no. Time containment belongs to each rig attempt's self-time budget;
   legitimate long-running trees have no ancestor-age deadline.
6. **Deadlock evidence for F** (found live, 2026-07-16 smoke): the local rig's
   flock lane pinning (`_pin_backend.sh`) holds a frame's backend lane for the
   frame's whole lifetime, INCLUDING while parked on dispatch. At depth 3 on
   the 2-lane stack: root holds lane A parked, child holds lane B parked,
   grandchild blocks forever in the third-branch blocking flock on lane A —
   circular wait, broken only by the rig timeout SIGKILL. The engine-side
   scheduler in F must own lane assignment and release a parked frame's slot
   (a parked pi is idle; the KV-cache warmth tradeoff is real but a
   timeout-deadlock is worse). Until F lands, local-rig jobs must keep
   dispatch depth ≤ 2.
