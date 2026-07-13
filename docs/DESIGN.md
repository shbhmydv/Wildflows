# wildflows — Design of Record (v1)

Expanded from `newsys_design_skeleton.md` (2026-07-12). Every **SETTLED** decision
(D1–D8) in that skeleton is law here and is preserved in spirit. Where the skeleton
is silent, the minimal call is marked **(hand-1 call, review pending)**.

> **Thesis.** The state machine is the hands, the model is the mind. The mind owns
> STRATEGY — which shape, what verification, when to end. The hands own EFFECTS —
> git, disk, journal, budgets. Grindstone's failure was not mediation; it was that
> control flow was *fixed*: the planner could diagnose a loop-shaped defect and had
> no verb but "another epoch." wildflows gives the mind an algebra of verbs.

---

## 1. The primitive algebra

A workflow is an **expression** over seven work primitives plus one ordering
combinator (`seq`) — eight expression kinds in all. Named shapes (swarm, battle,
senior→junior loop, grindstone-classic) are **macros** — saved expressions with a
name — and act as *nudges*: the planner reaches for a fitting macro first, composes a
wild expression only when nothing fits, and a wild expression that works is a
promotion candidate into the macro library (D2). No shape is whitelisted; bad
expressions are caught by the AUDIT run and the rails, not by a registry.

The seven primitives, each with inputs / output / failure mode:

### `do(task, rig, ctx)`
One agent, one task, in a fresh worktree.
- **Inputs:** `task` (natural-language instruction), `rig` (which harness/model
  executes it), `ctx` (context refs — files/results the core materializes into the
  worktree or the prompt).
- **Output:** a **result** — a `Result(text, files, ok)`. A diff, an artifact, free
  text, or a *judgment* are all the same shape; a judge is just a `do` whose task is
  "assess X" and whose output is a verdict. Nothing about judging is special.
- **Failure modes:** rig transport error (rate/session/crash) → BACKOFF (bounded,
  state-preserving, auto re-enter); non-zero rig exit or empty artifact → a result
  with `ok=False` carrying stderr tail; kill/timeout → the process is reaped, the
  worktree and journal survive, the node re-enters from its last durable point.

### `dispatch(tasks)`
Parallel `do()`s — **unordered by contract**. One worktree per child; children are
disjoint by construction and may run concurrently (real parallelism is ladder step 3;
the PoC executes them serially, but that order is an implementation detail, never a
contract). When execution order matters, wrap the steps in `seq`, not `dispatch`.
- **Inputs:** a list of `do` (or any) sub-expressions.
- **Output:** a list of results, positionally aligned with the inputs.
- **Failure modes:** per-child, as `do`. A child BACKOFF does not fail its siblings;
  the core integrates the ready children and re-enters the parked child. Integration
  of sibling diffs uses the **disjoint-ownership merge** (below).

### `seq(children)`
Strictly ordered execution: run each child, in list order, one after another. The
ordering combinator that `dispatch` deliberately is *not*. A loop `body` is typically a
`seq` (edit → build → judge, in order).
- **Inputs:** a list of sub-expressions.
- **Output:** the ordered list of child results.
- **Failure modes:** per-child, as the child's own kind; downstream children still run
  (the planner reads the journal and re-shapes) — `seq` sequences, it does not gate.

### `combine(results, task)`
A `do()` whose input is *other results* (collect, judge-panel synthesis, merge).
- **Inputs:** the upstream `results`, a `task` describing the synthesis, a `rig`.
- **Output:** one `Result`.
- **Failure modes:** as `do`. `combine` reads its inputs from the journal/artifacts,
  so a resumed `combine` re-reads durable upstream results, never live memory.

### `loop(expr, until, cap)`
Repeat a sub-expression until a condition holds or a cap is hit. The body **may** hold
a live session across iterations (the senior→junior pattern: a senior keeps context,
juniors fan out, the senior picks up junior failures).
- **Inputs:** `body` (an expression), `until` (a checkable condition — a `setup`-style
  command whose exit code is the predicate, or a planner-judged result flag), `cap`
  (max iterations — a rail).
- **Output:** the result of the last integrated iteration.
- **Failure modes / D5 (SETTLED):** live loop-session state dies with the process. On
  resume the loop **restarts its body from the last integrated iteration**, with the
  journal tail as the briefing. No session serialization, ever. The `cap` is enforced
  by the core regardless of what the body claims. A command predicate is a **read-only,
  idempotent check**: it runs in a durable lease and its exit code is accepted only when
  tracked/index and untracked state are byte-identical to the lease snapshot. Mutation is
  captured and reverted by the standard recovery transaction, then journalled as a failed
  evaluation and raised as a typed error. External effects cannot be mediated (the same is
  true of rig commands), so predicate authors own their idempotence. The exact redo window
  is **predicate-executed → `loop_iter` durable**; a crash there reruns the check while the
  verified in-tree postcondition makes that redo harmless to the worktree.

### `inplace(edit)`
The planner's own hands — a small fix with no sub-agent.
- **Inputs:** an `edit` = a set of `(path, content)` writes the planner authored
  directly (whole-file content; **(hand-1 call, review pending)** — a unified-diff /
  patch form is a later addition, whole-file write is the minimal core).
- **Output:** a `Result` describing what was written; `ok` reflects whether the core's
  commit succeeded.
- **Effects:** the **core** writes the files into the workdir and does `git add` +
  `git commit`. The model never runs git. This is the one primitive where the "agent"
  is the planner itself.
- **Failure modes:** a write outside the workdir is rejected (path-escape guard); an
  already-identical edit with nothing staged is a durable no-op result with `ok=True`.

### `ask(owner)`
The planner pings the owner with a genuine decision; the expression **parks** until
answered (answer arrives via the dashboard).
- **Inputs:** a `question` (+ optional structured options).
- **Output:** the owner's `answer` (text), materialized as a result.
- **Failure modes:** none deterministic — it blocks. A rail (deadline) may expire the
  ask; **(hand-1 call, review pending)** an expired ask journals `asked`→`answered`
  with a synthetic `answer=""` and `ok=False` so replay is total.

### `setup(cmd)`
A journaled host mutation: `npm ci`, boot a dev server a loop keeps warm.
- **Inputs:** a shell `cmd`, an optional `cwd`.
- **Output:** a `Result` with the command's exit code, stdout/stderr tails.
- **Effects:** runs on the host (not a worktree) because its purpose is host state.
  This is the one primitive that legitimately mutates outside a worktree; it is
  journaled precisely so resume can decide whether to re-run it. **(hand-1 call,
  review pending)** setup commands are declared idempotent-or-not by the planner;
  non-idempotent setups are not auto-re-run on resume, only surfaced.

---

## 2. The expression model (data)

An expression is a small **recursive Pydantic model** — a discriminated union on a
`kind` field. This is the data the planner emits and the core walks. (Concrete types
in `wildflows/expr.py`.)

```
Expr = Do | Dispatch | Seq | Combine | Loop | Inplace | Ask | Setup

Do:       kind="do"       task: str   rig: RigRef   ctx: list[CtxRef] = []
Dispatch: kind="dispatch" children: list[Expr]              # unordered / parallel
Seq:      kind="seq"      children: list[Expr]              # strictly ordered
Combine:  kind="combine"  task: str   rig: RigRef   inputs: list[Expr]
Loop:     kind="loop"     body: Expr  until: Until   cap: int
Inplace:  kind="inplace"  edits: list[Edit]                 # Edit = (path, content)
Ask:      kind="ask"      question: str   options: list[str] = []
Setup:    kind="setup"    cmd: str   cwd: str | None = None  idempotent: bool = True
```

Supporting types: `RigRef(name, params)` names a rig implementation and its config;
`CtxRef` is a tagged reference to a file path or an upstream node id; `Until` is a
predicate (`cmd` whose exit-0 means done, or `flag` meaning "planner-judged last
result ok"); `Edit(path, content)`.

Every node carries a stable **`node_id`** (assigned by the core when the expression
tree is admitted, deterministic pre-order path like `n0.1.2`). The cross-epoch join
key between the expression tree and the journal is **`(epoch, node_id)`**, NOT
`node_id` alone: one epoch's `n0` must never inherit an earlier epoch's `n0` result, so
replay scopes every folded fact by `(epoch, node_id)` (B3, hand-4). This is what makes
resume = "replay the log against the tree." A rails block will ride alongside the root
expression, not inside it (§4, deferred).

One expression tree = one **epoch** = one planner re-entry point + one durability
point (§3). The tree is validated by Pydantic on admission; `do`, `inplace`, `seq`, `dispatch`, and
`loop` (with a `cmd` predicate) are *executable* in the PoC, but all eight kinds are
*representable* so the journal vocabulary and the model are proven complete from day one.

### Code homes (hand-6 authority split, from the bloat audit)

The engine was split by **authority**, each home owning one thing so the planned
scheduler / worktree / planner / dashboard steps each depend on one narrow seam:

| Home | Sole authority |
|---|---|
| `engine.py` | epoch lifecycle, expression traversal, primitive orchestration |
| `admission.py` | dealias + deterministic ids + whole-tree validation (`admit_epoch`) |
| `projection.py` | the one live journal fold (`RunProjection.apply`), resume decisions, `ExecutionOutcome`, `replay` |
| `workspace.py` | the effect transaction (`WorkspaceEffects`: lease, git mediation, reconciliation, failure revert+capture, containment) + `CompletionRecorder` (event ordering) |
| `result.py` | the `Result` / `Outcome` artifact value + `CommitReceipt` / `IntegrationReceipt` effect record |
| `journal.py` | the single append owner: seq-assign + fsync + `projection.apply` |

There is exactly ONE state system: the journal's live `RunProjection`, folded by one
`apply(event)`. Load replays the ndjson through the same `apply`, so a running
projection and a reloaded one are bit-identical (proven by the frozen journal fixtures
in `tests/fixtures/journals/`). Resume/durability is one decision — `resume_action` — over
an explicit attempt/iteration `floor` (`-1` top-level, an int for a resumed-partial loop
iteration, `None` for a fresh one) that replaces the old `_NO_RESUME` sentinel.

### Admission (hand-6) — one pass before `boundary(opened)`

`admission.admit_epoch(tree, epoch, projection, registry)` runs BEFORE any execution
event: it dealiases (wire round-trip), assigns deterministic node ids, then makes ONE
whole-tree traversal to reject a plan the core can refuse **without effects**. On
rejection it raises **`AdmissionError`** (a `ValueError` subclass) and NO journal event
is written — a representable-but-unrunnable shape never opens an incomplete epoch, so
`NotImplementedError` after a durable boundary is gone. Admission rejects:

- **executor capability** — `combine` / `ask` / `setup`, and `loop` with a `flag`
  predicate (representable, not yet executable);
- **unknown rig names** — every rig-bearing node's `rig.name` must resolve in the registry;
- **ctx node refs** — a `CtxRef(kind="node")` must name a node in the epoch tree;
- **resume identity** — on an already-open epoch, the supplied tree must equal the
  journalled `boundary(opened).expr` (a divergent resumed tree is a caller error).

**Local Pydantic invariants** (single-model, checked at construction) carry the rest:
lexical path guards on `Edit.path` and file `CtxRef.ref` (leading dash, absolute, `..`,
literal `.git`), duplicate paths within one `Inplace`, positive `loop.cap`, and positive
rig `timeout_s`. **Environment-dependent** checks stay at use time in `Workspace` (symlink
escapes, the resolved linked-worktree gitdir, a not-yet-created ctx file, git/process
failures, the actual `until` result) — a validator cannot resolve a symlink.

---

## 3. Runs, epochs, and the event vocabulary

### Epoch boundary
An epoch boundary is exactly two things (skeleton §2): the planner **RE-ENTRY** point
(look at results → next expression / different shape / end) and the **DURABILITY**
point (journal flush; a baton is optional, the planner's choice). No mandatory
close-out, no cadence. ~90% of runs are 1–2 epochs.

### The single event vocabulary (SETTLED invariant 2)
Every primitive execution is **one event** in **one** vocabulary. Resume replays this
log against the expression tree; there is no per-shape resume code. The concrete event
types (in `wildflows/events.py`), each a Pydantic model sharing a header
`(seq, ts, run_id, epoch, node_id, kind)`:

| event        | emitted when                              | key fields |
|--------------|-------------------------------------------|------------|
| `boundary`   | an epoch opens / closes                   | `phase: opened\|closed`, `expr` (opened: the admitted tree) |
| `dispatched` | a `do`/`combine`/`inplace`/`setup` starts | `rig`, `task`/`cmd`, `workdir` |
| `result`     | an agent/primitive produced output        | `outcome` (source of truth; `ok` derived), `text` tail, `files` (artifacts), `exit_code?`, `loop_status?` |
| `integrated` | the core applied+committed a result       | `commits` (every attributed commit + its paths; `commit`/`paths` derived) |
| `judged`     | a `do`-as-judge produced a verdict         | `verdict`, `ok`, `target_node` |
| `loop_iter`  | a `loop` completed one iteration          | `iteration`, `commit`, `converged` (body outcome by reference, no payload) |
| `asked`      | an `ask` parked                           | `question`, `options` |
| `answered`   | the owner answered (or the ask expired)   | `answer`, `ok` |

Notes:
- `judged` is a *specialization* of `result` for the judge case, kept distinct so the
  dashboard and the AUDIT macro can filter verdicts without parsing task text. A judge
  emits `result` (its raw output) **and** `judged` (the extracted verdict). **(hand-1
  call, review pending)** — the skeleton lists `judged` in the vocabulary but does not
  say whether it co-exists with `result`; co-existing keeps `result` universal.
- `setup` uses `dispatched` + `result` (exit code in `result.exit_code`); no special
  event, per "one vocabulary."
- `loop_iter` is one event per completed loop iteration, carrying the iteration index,
  the workdir HEAD after the body integrated, and whether `until` converged. **(hand-2
  call, review pending)** — a dedicated event was chosen over an `iteration` field
  smeared across every event: the loop is the only primitive that repeats a node, so a
  per-repeat fact belongs to its own event, keeps every other event single-shot, and
  lets replay expose "iterations-completed + last commit" (D5) in a two-line fold with
  no special case. Each iteration's inner nodes still emit their normal
  `dispatched`/`result`/`integrated`; `loop_iter` is the per-iteration cap/convergence
  marker over them, not a replacement. **`loop_iter` carries NO body-artifact payload
  copy (hand-7, item 3): it references the body outcome by journal position** — the body
  leaf's `result` is the last one folded before the `loop_iter`, so the projection
  recovers the iteration's body from its live last-result at fold time (old journals with
  legacy `body_*` fields fold identically, since that same preceding `result` is present).
- **Completion ordering (hand-7, item 4): ONE recorder, one order — `result` THEN
  `integrated`, for every path** (do, inplace, reconciliation). This replaces three
  inconsistent orderings. Result-before-integrated is the torn-tail contract: an effectful
  result without its `integrated` reads as NOT durable (it re-runs / reconciles), never a
  lost or duplicated effect.
- `integrated` is emitted only by the **core**, never a rig — it is the mediation
  proof (invariant 1). This holds for **`do`** too (hand-4, B5): after a rig runs, the
  core stages + commits the worktree's changes (`git add -A -- .`, message keyed by
  node_id) and emits `integrated`. **The `integrated` event carries an
  `IntegrationReceipt` — EVERY commit in `pre..post` (a rig may legitimately author
  several), each with its own changed paths (hand-7, item 3 / defect 4)**; `commit` (the
  final sha) and `paths` (the order-preserving union — the disjoint-ownership set) are
  derived. Replay **accumulates** a node's receipts (union of commits) — no last-write-wins
  on a single `paths` list. An effectless `do` (no diff) legitimately produces no
  `integrated`. **Result vs effect (hand-7):** `Result.files` is the AGENT ARTIFACT list
  (== the effect paths in the shared-workdir PoC, distinct once per-node worktrees land);
  the `IntegrationReceipt` is the ownership/commit ledger, and durability keys off the
  receipt, not the artifact list.
- **`result.outcome` is the single terminal-status source; `ok` is a derived convenience
  (`outcome == "ok"`)** — the ok/outcome duplication is collapsed (hand-7, item 3). A loop
  that hits its cap without converging is `outcome="failed"`. See the compatibility note (§6).
- A **`loop`**'s final `result` carries the last integrated iteration's body artifact in
  `text`/`files`; the convergence/cap disposition rides in the SEPARATE `loop_status`
  field (SF6, hand-4), so a downstream `combine` consumes the artifact, never the status
  prose. `loop_status` is `None` on every non-loop result and is journal-only (the
  dashboard reads it; replay's `Result` reconstruction ignores it).
- Sequence numbers (`seq`) are a strictly increasing per-run integer; the journal is
  append-only ndjson plus an in-memory mirror (§6).

### Honesty rule
The only cross-cutting truth requirement (skeleton §3): what the expression *declared*
actually ran, and the journal shows which. No mandatory critics, close-outs, batons,
or gates — those are the mind's choices, not invariants.

---

## 4. Rails (SETTLED invariant 3) — DEFERRED to ladder step 4

The planner will declare rails **up front** with each epoch and the **core will
enforce** them — a confidently-wrong mind needs a wall. The intended shape:

```
Rails: budget_usd: float | None      # cumulative rig spend cap
       deadline_s:  float | None      # wall-clock from epoch open
       iter_cap:    int  | None       # loop iteration ceiling (per loop node)
```

**Rails are NOT admitted, recorded, or enforced yet.** Rails admission (a validated
block riding the `boundary(opened)` event) and enforcement land **together at ladder
step 4** (worktree hygiene), so the executor never records a rail it cannot enforce
nor enforces one it never admitted. The speculative `Boundary.rails: dict|None`
placeholder field was DELETED in hand-6 (an un-admitted, un-enforced raw dict is the
opposite of §4's rule); the typed rails block is added when admission + enforcement land
together. Until then, **the only live rail is the executable `loop`'s `cap`** — a `loop` cannot exceed `node.cap` (core-enforced; cap-exhaustion is
an `ok=False` result, never a crash). Budget, deadline, and `iter_cap` refusal
semantics do not exist in the current engine. When they land, the chosen semantics are
*refuse-to-start* (a node that would breach a rail is refused and the epoch closes with
`boundary(closed, reason=...)`); an in-flight deadline kill is a step-4 concern. **(SF4,
hand-4: rails deferral confirmed on owner review — do not read §4 as currently enforced.)**

---

## 5. Resume semantics (SETTLED invariants 2 & D5)

Resume = **replay the ndjson against the expression tree**. There is ONE re-entry path
(grindstone's 76d01fe lesson): on start, `Engine` loads the journal (seq continues
strictly-increasing across restarts, B1), folds it into per-`(epoch, node_id)` state,
and `run_epoch` re-enters. A fully-closed epoch is a no-op; an opened-but-unclosed epoch
resumes **without a second `boundary(opened)`**; a fresh epoch opens. Per-primitive
replay rules:

- **`do` / `combine` / `setup`:** a node with a durable `result` is **done** — BUT a
  node with **declared file effects** (a non-empty `result.files`) is durable only once
  the core's `integrated` (the committed diff) is journalled; a result without its
  `integrated` is **NOT durable** and re-runs (B5). An **effectless** node (no diff) is
  durable on its result alone. A node `dispatched` without a `result` is **in-flight** →
  re-dispatch (the prior worktree/process is dead; the reaper guarantees no orphan
  mutates git). The rig's claim is never the durability record — the committed diff is.
  **Two-boundary provenance (hand-9, §Appendix 41):** the `result` event is a SECOND
  durable boundary carrying `post_head`; a torn result-then-integrated window reconstructs
  the receipt from EXACTLY `pre_head..post_head` (never `..HEAD`). A dispatched-only tail has
  no `post_head` certificate — it is ALWAYS re-run. **Quarantine, never destroy (hand-10,
  §Appendix 47):** before the re-run, the dead attempt's tip (its mid-rig commits AND any
  post-crash operator commit) is moved to a quarantine ref and the branch reset to the
  DURABLE `pre_head` (loaded from the on-disk lease record, not memory); uncommitted dirt +
  non-preexisting untracked leaks are captured to run_dir; pre-existing untracked files are
  left in place. Because the re-run starts from `pre_head`, an idempotent rig can never
  absorb a retained commit as unreceipted success — it must author its own effect and earn
  its own receipt. The re-run's lease still REFUSES to open on a dirty tracked/index
  worktree (the honest serial-M1 rule); every cleanup/rollback git op is CHECKED and a
  failure HALTS the epoch (a `workspace_unclean` failed result), never a durable "failed"
  that lies the effect was reverted.
- **`inplace`:** `integrated` present → done (the commit is durable); `dispatched`
  without `integrated` → re-apply the edits (whole-file writes are idempotent). **Durable
  intent (hand-10, §Appendix 47):** before the first write, the per-path original content is
  fsynced to `run_dir/intents/`; a crash mid-edit is REVERSED from that record on restart
  (a partial write to a pre-existing — possibly untracked — file is restored, which a
  `reset --hard` alone cannot recover) before the node re-runs. Any exception after the
  first write (not just a symlink `ValueError` — an `OSError` too) rolls back from the same
  record. An empty/no-diff inplace is a durable no-op.
- **`dispatch`:** fold each child independently; integrate ready children, re-dispatch
  in-flight ones.
- **`loop` (D5):** live session state is gone. Find the **last integrated iteration**
  from the journal (the count of `loop_iter` events); restart the body from the next
  iteration with the **journal tail as briefing**. The `cap` counts integrated
  iterations, so a resume cannot exceed it. **Partial-iteration fold rule (B4, hand-4):**
  the resumed iteration is *partial* — some inner nodes ran before the process died.
  Inner-node state journalled **at or before the last `loop_iter` event** belongs to a
  COMPLETED iteration and does NOT satisfy resume for the partial one; only inner state
  journalled **after** the last `loop_iter` is durable for the current iteration. The
  engine implements this by passing the last `loop_iter` seq as a resume *floor* into the
  partial iteration's body (state `seq <= floor` is stale); every subsequent fresh
  iteration re-runs its whole body (`floor=None`, the explicit never-resume scope).
  Predicate completion is intentionally durable only with the corresponding `loop_iter`:
  a crash after the leased check executes but before that append reruns the idempotent
  predicate, after recovering any unfinished predicate lease, without rerunning a durable
  body leaf.
- **`ask`:** `asked` without `answered` → still parked; re-surface to the owner.
  `answered` present → the answer is durable, continue.
- **`boundary(opened)` without `boundary(closed)`:** the epoch is incomplete; resume
  inside it. A closed boundary means advance to the next planner re-entry.

No session serialization, no per-shape resume implementation. The reaper reaps
processes only — it never mutates git or disk (grindstone's hardest-won kill-hardening
lesson), so evidence never dies with a kill.

---

## 6. The journal (in-memory + ndjson)

The journal is the run's spine. In the PoC it is both an **in-memory append-only list**
and an **ndjson file** at `<run_dir>/events.ndjson`, written line-per-event with an
fsync-on-append discipline. Each event is a Pydantic model serialized with its `kind`
discriminator; loading re-parses each line back into the typed union. The journal is
the durable state the projection/dashboard consume. Effect recovery additionally
consumes integrity-bound lease, intent, recovery, and settled-lease records under
`run_dir`; those records are transaction inputs/certificates, while lifecycle decisions
still fold from the journal. Git tips and artifacts hang off node_ids recorded there.
The journal exposes:
`append(event) -> seq`, `events() -> list[Event]`, and `load(run_dir) -> Journal`.

**Event shape versioning + compatibility (hand-7).** Item 3 changed three event shapes
(collapsed `result.ok`→`outcome`; `integrated.commit`/`paths`→`commits`; dropped
`loop_iter.body_*`). There is no explicit schema-version integer yet (a later dashboard
call); instead each changed model owns a `mode="before"` **compatibility reader** so an
OLD journal folds unchanged: a bare `ok=False` (or the old loop-cap `ok=False,
outcome="ok"` drift line) migrates to `outcome="failed"`; a single-commit `integrated`
migrates to a one-element `commits`; a legacy `loop_iter.body_*` payload is ignored (the
preceding `result` reconstructs the body). The frozen `tests/fixtures/journals/*.ndjson`
are genuinely old-shape and prove these readers on every run.

**Torn-tail tolerance (B2, hand-4):** a kill/power-loss during the final `write()` can
leave the last ndjson line unterminated or malformed. `load` drops exactly that one
torn FINAL record (it never durably completed, so the next append reuses its seq) and
still raises on any malformed COMPLETE or MIDDLE record. fsync-on-append bounds the
damage to the last line; it does not eliminate a partially written last record.

**Single-writer precondition (N2, hand-4):** the journal derives `seq` as one past the
last loaded/appended event and has **no lock or writer queue**. `Engine`-load-continues-seq (B1)
covers **serial restarts only** — one append owner at a time. Parallel dispatch (ladder
step 3) MUST introduce a single central append owner or an interprocess lock before any
concurrent children/processes append; two live `Journal` instances over one run_dir
would emit duplicate/reordered seqs. Not built this hand.

**Failed-append poisoning (hand-14):** sequence assignment is serialized on a copy and
live `_events`/projection state changes only after the line's write+flush+fsync (plus the
first-file directory fsync) succeeds. Any durability exception leaves the live mirror at
its prior durable prefix and permanently poisons that `Journal`; every later append raises
`JournalPoisonedError`. Only `Journal.load` may classify the physical tail (the failed
fsync may nevertheless have left a complete line) and create a fresh append owner. M2's
central owner must discard, never retry through, a poisoned instance.

---

## 7. Worktree mediation & the disjoint-ownership merge (target invariant 1)

**Target after worktree isolation lands:** every `do`/`dispatch` child runs in its own
Git worktree off the run's base commit. The core integrates each child and enforces
**disjoint ownership**: two integrated siblings may not both modify the same path.
`inplace` and `setup` remain the exceptions.

**Current serial PoC:** `do` and `inplace` share the integration workdir; a rig may author
commits there, and the core validates, attributes, receipts, or quarantines them. Worktree
creation, disjoint merge, reaping, and discard replace this shared-workdir recovery policy
at the later hygiene step. `Rig.run(prompt, workdir)` is the seam that isolation slots
behind; this section's first paragraph is not a claim that isolation is implemented now.

---

## 8. Rigs (the multi-harness seam)

A **rig** is a harness+model that executes a `do`. The seam is one method:

```
Rig.run(prompt: str, workdir: Path) -> Result
```

prompt in, text/files out — exactly grindstone's shape-agnostic `request.sh` contract,
which is why real rigs (`claude -p`, `pi`, local Qwen, `codex exec`) plug in later with
no engine change. The PoC ships two:
- **`EchoRig`** — deterministic, returns a canned/derived result; the test substrate.
- **`ShellRig`** — shells out to an arbitrary command template (e.g.
  `claude -p {prompt}` run in `{workdir}`), capturing stdout/exit as the result. This
  is the real plug-in path; **real rigs are NOT integrated now** (no network, no model
  calls in this build).

A `RigRef(name, params)` in an expression names which rig and its config; the core
resolves it through a **rig registry** at execution time. Rigs never touch integration
git; they only write inside their `workdir`.

### The script contract — THE integration seam (grindstone-compatible by construction)

`ShellRig` proves the shell-out shape; the production seam is **`ScriptRig`**, which
drives a configured executable through the exact contract grindstone's rigs already
use, so a real script (`models/picodex/senior_request.sh`, `planner_request.sh`, the
`codex`/`claude`/local-Qwen rigs) plugs in with no engine change:

```
<script> --worktree <dir> --prompt <file> --log-dir <dir> \
         --handle-out <file> --timeout <secs>
```

The script grinds agentically **inside the worktree**, commits its own work (the
deterministic gate is the committed diff, never a handoff file), propagates its exit
code, and prints rate/session-limit signatures on stderr. `ScriptRig` classifies that
exit + stderr into a typed **`Result.outcome`** — `ok` / `failed` / `busy`, where
`busy` is a rate/session/quota wall that must NOT read as a task failure. Contract
notes that are load-bearing:

- **`--prompt` is a FILE PATH, not inline argv.** The real rigs feed the prompt to the
  CLI on stdin from that file to dodge the kernel's `MAX_ARG_STRLEN` (~128 KB) wall on
  large prior-failure context. `ScriptRig` writes the prompt to
  `<log_dir>/<dispatch>/prompt.txt` and passes its path.
- **Per-dispatch log dir** is `<log_dir>/<workdir.name>/`, populated with the captured
  `agent.stdout.log` / `agent.stderr.log` (+ the prompt) so the dir is non-empty even
  when the script writes nothing. In the real worktree seam (step 4) a `do` runs in a
  worktree named for its node_id, so the dispatch key IS the node_id by construction.
- **`busy` is journalled as `ResultEvent(ok=False, outcome="busy")`** — distinct from a
  failure so a later ladder step can back off + re-enter. No backoff/retry policy is
  built in this hand.
- Real scripts live OUTSIDE this repo; `examples/rigs.yaml` ships the `script` rig
  commented out. Rigs are declared in an owner-facing **`rigs.yaml`** (YAML by policy),
  a Pydantic discriminated union (`echo|shell|script`) loaded by
  `load_rigs(path) -> RigRegistry` — unknown kinds are rejected at load.

---

## 9. The BUILD + AUDIT run macro (SETTLED §2)

A run defaults to **two runs in sequence** (a macro, not an invariant):
- **BUILD run** — whatever expressions get the job done; verification as light as the
  planner dares (a failing-test contract inside a `do`, one judge over a swarm,
  self-verify, or nothing).
- **AUDIT run** — `combine(dispatch(expert-lenses), synthesize)` over the **artifact**,
  not the tasks. The panel is spec-UNBOUND and build-blind: it uses the product like
  its users (play the lessons, render screens, break flows, fresh-eyes architecture
  review). Output = a punch list → a fix expression, or "ship." Trivial runs skip it.

Audit lenses are planner-chosen from a **lens library** (D4); the run-16 seed lenses
are kid-simulation, fresh-eyes-architecture, and render-sweep. The user's job-spec
epoch/shape sketch is the **strongest suggestion** (D3): the planner may deviate with a
journaled rationale. The user's `done_when` acceptance, if given, runs **once at END**
(invariant 4); with no acceptance, planner judgment is the exit. This macro is not in
the PoC engine (it needs `dispatch`/`combine`) but the event vocabulary already carries
everything it emits (`judged`, `result`, `boundary`).

---

## 10. The dashboard (skeleton §7) — a pure journal consumer

The dashboard is a **first-class citizen**, not grindstone's view-only `dash.py`. A
modern app you *use and control the run through*: live expression tree + journal,
pause/resume a loop, retry a hand, kill an expression, answer `ask(owner)` questions
(push-notification deep-links land here), inspect any `do`'s handoff/artifacts/renders
inline. It is built at ladder step 6, but the **event vocabulary is designed from day
one so the dashboard is a pure consumer of the journal** — no engine surgery later.
Every control it offers maps to a journal fact: the expression tree is the
`boundary(opened).expr`; progress is the fold of `dispatched`/`result`/`integrated`
per node_id; a parked `ask` is an `asked` with no `answered`. Because the journal is
the single source of truth (§6), the dashboard reads the same ndjson resume reads.

---

## 11. Dogfoods — system acceptance criteria (skeleton §5)

The system is "done enough" when it closes both, each ending with an AUDIT run (the
panel macro's first live exercise):

- **DF1 (loop shape).** The banked arc-stage clipping family in the RN app
  (KNOWN-DEFECTS @ 06452897): retina/short-viewport clipping + remount-on-remeasure.
  Expression: `loop(senior with a live dev server + export; render → measure real DOM →
  fix → re-render, until the viewport matrix + hang-guard are green, cap N)`. **Success
  bar:** closed in ≤ ~30 min wall-clock (grindstone baseline: 8 epochs / ~5h, failed).
- **DF2 (swarm shape).** Run-17 asset quality. Expression: `inventory →
  dispatch(per-asset download-or-author) → combine(judge panel) → manifest lint`.
  **Success bar:** every graded-bad asset replaced, licenses receipted.

Historical build order (D7/D8, bottom-up): (1) expression PoC; (2) durability;
(3) composition + rails; (4) worktree hygiene; (5) target-local `.wildflows/`; (6)
dashboard. **Current status:** the serial engine has fsynced journal replay, durable
lease/intent/recovery transactions, and executable `do`/`inplace`/`seq`/serial-`dispatch`
/command-`loop`. `combine`, general rails, real parallel dispatch, per-node worktrees,
target-local run state, and the dashboard remain later steps.

---

## Appendix: hand-1 calls pending review

1. `inplace` uses whole-file `Edit(path, content)` writes, not unified diffs (minimal
   core; diff form is a later, additive change).
2. `judged` co-exists with `result` (a judge emits both) so `result` stays universal.
3. Rails enforce by **refuse-to-start**, not in-flight interrupt (determinism; in-flight
   deadline kill is a step-4 concern).
4. An expired `ask` journals a synthetic empty `answered(ok=False)` for total replay.
5. `setup` carries a planner-declared `idempotent` flag; non-idempotent setups are
   surfaced on resume, not auto-re-run.
6. **REVERSED on review: `seq` added, `dispatch` is parallel-only.** Hand-1's call to
   overload `dispatch` as a sequential ordering container was rejected. `seq` is now an
   explicit 8th expression kind for strict ordering; `dispatch` returns to
   unordered-parallel semantics (the PoC may still execute dispatch children serially,
   but that is an implementation detail, not a contract). This is a settled reversal,
   not a pending call.

### Hand-2 calls pending review

7. **`loop_iter` event** for per-iteration journalling (see §3 note): a dedicated event
   over an `iteration` field on every event, so replay folds iterations-completed + last
   commit in two lines and every other event stays single-shot.
8. **`_commit` checks the STAGED diff only** (`git diff --cached --quiet`), not the whole
   working tree (`git status --porcelain`). **The predicate rationale here is REVERSED by
   hand-14 entry 67:** `until` may no longer leave untracked artifacts. Staged-only remains
   the correct "did the declared edits change anything" test and keeps `inplace` re-apply
   idempotent.

### Hand-3 calls pending review

9. **`Result.outcome` discriminator (`ok`/`failed`/`busy`)** added alongside `ok`,
   mirrored on `ResultEvent` (defaults to `"ok"` so pre-existing journal lines parse
   unchanged). A `busy` (rate/session/quota) wall journals `ok=False, outcome="busy"`
   so the engine does not confuse a transport wall with a task failure — the minimal
   representation until a real backoff/re-entry ladder step is built.
10. **`ScriptRig` per-dispatch log dir keyed by `workdir.name`**, not an explicit
    node_id param — the `Rig.run(prompt, workdir)` protocol carries no node_id, and the
    real worktree seam (step 4) names each `do`'s worktree for its node_id, so the key
    is node_id by construction. Revisit if step 4 names worktrees differently.
11. **Timeout is represented as `outcome="failed"` + a `[timeout]` text marker**, not a
    fourth outcome — the caller treats a timeout like any other non-busy failure.
    `ScriptRig` kills the direct child only (`subprocess` timeout); reaping the process
    GROUP is a step-4 (worktree hygiene) concern.
12. **`ScriptRig` mirrors `senior_request.sh`'s arg contract** (`--worktree`/`--log-dir`
    /`--handle-out`), NOT `planner_request.sh`'s (`--repo`/`--workdir`/`--out`). The two
    reference scripts diverge: the planner adds `--out` (a decision.json fallback read
    channel) and `--purpose`, and renames `--worktree`→`--workdir`, `--log-dir`→derived.
    The executor/worker contract (senior) is the general seam; a planner rig is a
    later, distinct role with its own extra flags.

### Hand-4 calls (from external review) pending review

13. **`(epoch, node_id)` is the replay join key** (B3), not `node_id` alone: every
    folded fact (result/integrated/dispatched/loop) is scoped by epoch, and epoch
    open/closed folds to the LATEST boundary event for that epoch, so a reopened epoch
    is never reported closed and one epoch's node never inherits another's result.
14. **`do` is core-integrated like `inplace`** (B5): the deterministic durability record
    is the committed diff the core makes (`git add -A -- .` + commit), never the rig's
    claim. Fold rule: a result with declared file effects (`files` non-empty) is durable
    only with its `integrated`; an effectless `do` stays durable on its result alone.
15. **`inplace` commits ONLY its declared paths** (B6) via a `--`-scoped pathspec commit
    (`git commit -- <paths>`), preserving any pre-existing staged index; edit paths are
    literal (leading-dash rejected at admission), and `.git`/resolved-gitdir writes are
    refused (N1). Git failures inside integration become a journalled
    `result(ok=False, outcome="failed")` carrying git stderr — never an escaping
    exception (SF1); an empty `inplace` is a no-op ok result with no git calls.
16. **Loop partial-iteration resume floor** (B4): inner-node state at/below the last
    `loop_iter` seq is stale for the partial resumed iteration; the engine threads that
    seq as a resume *floor* (fresh iterations use +∞). See §5.
17. **Loop result = last integrated body artifact; `loop_status` holds the disposition**
    (SF6): the loop's final `result` carries the body's `text`/`files`; the
    convergence/cap string lives in the separate `loop_status` field so a downstream
    `combine` receives the artifact, not prose. See §3.
18. **`ShellRig.timeout_s` is required** (SF3): no rig may be unbounded; a timed-out
    command is reaped and returned as `outcome="failed"` with a `[timeout]` marker. The
    engine additionally wraps every `rig.run` in try/except → a journalled failed result
    (never an escape after `dispatched`). A `cmd` `Until` without a `cmd` is rejected at
    admission (SF5). `RigRef.params` is admitted but NOT yet consumed (reserved for the
    planner-config seam, §8) — narrowed rather than implemented this hand (SF2).

### Hand-5 calls (from pass-2 external review) pending review

19. **Resume identity (NB1).** `run_epoch` on an already-OPEN epoch canonically compares
    the supplied tree (post-dealias, post-id-assignment `model_dump`) against the
    journalled `boundary(opened).expr`; a mismatch RAISES before any execution. The
    planner re-shapes at epoch boundaries, never mid-epoch, so a divergent resumed tree
    is a caller error, not a shape the executor should silently run under an open epoch.

20. **Dealias on admission (NB2).** `run_epoch` re-parses the supplied tree through the
    wire model (`parse_expr(tree.model_dump())`) before id assignment, so two positions
    that shared one Python instance become distinct objects and `assign_node_ids` can
    never collapse two declared nodes onto one journal key. **Deviation from the owner
    triage:** the triage prescribed `model_copy(deep=True)`, but Python's `deepcopy`
    preserves shared references (memoized), so an aliased child stays aliased and the
    collapse persists — verified empirically. The model round-trip genuinely dealiases
    (serialization emits each occurrence; deserialization builds fresh objects) and also
    deep-copies, so the caller's tree is never mutated. The reviewer's regression rides
    on top and passes.

21. **`do` reconciliation rule (NB4).** Every commit the CORE makes carries a
    machine-parsable marker `wf:<run_id>:<epoch>:<node_id>` in its message. On resume,
    before re-running an in-flight `do`/`inplace`, the core scans `git log` for that
    marker; if a matching commit exists (the commit-then-crash window: git committed but
    the journal write did not), it retro-journals `result`+`integrated` FROM that commit
    instead of re-executing — no duplicated or lost effect. Reconciliation runs only for
    TOP-LEVEL nodes (resume floor `-1`); a loop body's per-iteration commits reuse the
    same marker, so loop resume is owned by the loop fold (NB3/B4), not the marker scan.
    **Reachability rule (hand-7, defect 1):** the marked commit must be REACHABLE from
    current `HEAD` (the scan is `git rev-list --grep=<marker> HEAD`, which walks only
    HEAD's ancestry). A marked commit on an unrelated/side branch — absent from the
    worktree — is NOT retro-integrated; it is left to normal re-execution, never false
    durable attribution of an effect the worktree does not contain.

22. **Rig-commit recording + shared-workdir reset policy (NB5).** After every `do` the
    core snapshots pre-run HEAD. On SUCCESS, commits the rig itself made (`pre..HEAD` —
    the senior/script contract legitimately commits its own work) are journalled as
    `integrated` attributed to that node; the core then integrates any remaining dirty
    state. On FAILURE (non-zero rig or exception), the dirty working-tree diff is
    captured verbatim to `<run_dir>/failed-diffs/e<epoch>-<node_id>.diff` (the path rides
    in the failed `result.text`) and the workdir is RESET to HEAD, so no later node can
    stage and claim the leak as its own integration. This is a **PoC shared-workdir
    policy**, superseded by per-node worktrees at ladder step 4.

23. **`ctx` file containment (NB6).** A `CtxRef(kind="file")` is subject to the same
    containment guard as `inplace`: the resolved path must be relative to the workdir and
    must not touch `.git`. An absolute path, `../` escape, or in-worktree symlink pointing
    outside is a FAILED result at exec time (admission cannot resolve symlinks), never a
    host-file read transmitted into a rig prompt.

24. **`loop_iter` carries the body artifact (NB3/SF6).** `loop_iter` gains additive
    `body_text`/`body_files`/`body_exit_code` fields. On resume, a journalled
    `converged=True` loop_iter (or a final capped iteration) short-circuits: the loop
    result is reconstructed from the journalled last-body facts with NO body re-run.
    Pre-existing loop_iter lines default these fields to empty, matching old behavior.

25. **Non-empty no-diff `inplace` is a durable no-op (SHOULD-FIX 4).** An `inplace` whose
    edits produce no git diff (content already identical) journals a durable no-op —
    `result(ok=True, files=[])`, no `integrated` — so `_is_durable` accepts it on its
    result alone and resume never re-applies it. This is the representation DESIGN §5's
    "empty/no-diff inplace is a durable no-op" now points at for the non-empty-edits case.

26. **Robustness fixes.** Torn-tail load reads RAW BYTES and drops the final record only
    when it lacks a terminating newline AND fails to parse (a newline-terminated invalid
    line is corruption → raise; a mid-UTF-8 unterminated tail recovers) (SHOULD-FIX 1/B2).
    An unknown rig name is a journalled failed result, not a crash (SHOULD-FIX 2). A
    `ShellRig` timeout kills the process GROUP (`start_new_session` + `killpg`) so
    backgrounded children are reaped (SHOULD-FIX 3). A blank/whitespace `Until.cmd` is
    rejected at admission (SHOULD-FIX 5). Core integration parses changed paths NUL-
    delimited (`-z`) so whitespace filenames stay one path (SHOULD-FIX 6). A `ShellRig`
    non-zero exit sets `outcome="failed"` (SHOULD-FIX 7).

27. **N2 (single-writer journal) remains DEFERRED to ladder step 3** (§6) — documented,
    not implemented this hand: the current fixes harden the SERIAL restart/effect
    invariants that step 3's parallel dispatch would otherwise build on unsoundly.

### Hand-6 calls (from the bloat audit) — an equivalence refactor

28. **Authority split (hand-6, from bloat audit).** The 700-line `Engine` was split into
    five homes — `engine.py` (orchestration), `admission.py`, `projection.py`,
    `workspace.py`, `result.py` — plus the journal as the single append owner (see §2
    "Code homes"). Behavior is bit-for-bit identical; tests changed only for imports and
    the sanctioned admission-rejection API (below). No executor-per-primitive files, no
    visitor classes, no plugin framework.

29. **One live `RunProjection` (hand-6).** The frozen `_state` snapshot + raw-journal
    scans (`_is_durable`, `_last_result_since`, `_journalled_result_text`, per-entry
    refolds) are replaced by ONE `dict[NodeKey, NodeProjection]` folded by a single
    `apply(event)`, owned by the journal and updated on every append; load replays
    through the same `apply`. `resume_action(key, floor)` is the single durability
    decision and the `_NO_RESUME = sys.maxsize` sentinel becomes an explicit `floor=None`
    scope. Proven equivalent by re-folding pre-refactor journal fixtures
    (`tests/fixtures/journals/`) to identical snapshots. Event shapes are UNCHANGED
    (`LoopIter.body_*` stay for a later hand). The loop still consumes its body's "last
    result" (now via the projection, not a journal slice); the full outcome-reference
    model is a later raze.

30. **Admission pass (hand-6, sanctioned behavior change).** `admit_epoch` runs before
    `boundary(opened)` and rejects capability / unknown-rig / bad-ctx-node-ref / resume-
    identity errors with `AdmissionError` (see §2 "Admission"). This SUPERSEDES the
    runtime treatment of these inputs: entry 26's "unknown rig name is a journalled failed
    result" and the old runtime `NotImplementedError`/failed-result for unexecutable kinds,
    missing ctx node refs, and absolute/`..` ctx-file paths are now admission rejections
    BEFORE any event. Lexical path guards moved to `Edit`/`CtxRef` validators; duplicate
    inplace paths and positive rig `timeout_s` are new local invariants.

31. **Speculative-field/residue deletions (hand-6, low-risk rows only).** `Boundary.rails`
    (un-admitted raw dict, §4) deleted; the review-ticket comment labels
    (`B*`/`NB*`/`SF*`/`SHOULD-FIX`) stripped from source (history lives here); the stale
    `expr.py` docstring corrected. `RigRef.params`, the second `Judged` event,
    `LoopIter.body_*` and the `ok`/`outcome` duplication were intentionally left at that
    point (then removed by later hands); the historical `_capture_and_reset_dirty` helper
    was likewise deleted when `recover_lease` became the sole transaction in hand-12.

### Hand-7 calls (RAZE items 3+4 + pass-3 review) pending review

32. **Result / effect / outcome separation (item 3).** (a) `Result.outcome` is the single
    terminal-status source; `ok` is a derived `computed_field`, collapsing the ok/outcome
    duplication (mirrored on `ResultEvent`). (b) The `integrated` event carries an
    `IntegrationReceipt` = every commit in `pre..post` with per-commit paths; replay
    ACCUMULATES a node's receipts (no last-write-wins on `paths`). `Result.files` is now
    framed as the artifact list, the receipt as the ownership ledger; durability keys off
    the receipt. (c) `_exec` returns an `ExecutionOutcome` — a leaf's result key, or
    position-indexed child references for `seq`/`dispatch`; the loop reads its body's
    outcome through that reference (the hand-6 `last_result_since` bridge is deleted) and
    `loop_iter` drops its `body_*` payload copy for a by-position reference to the body's
    journalled `result`.

33. **Workspace effect transaction + completion recorder (item 4).** `WorkspaceEffects`
    owns the per-node-attempt lease (pre/post HEAD over the shared workdir — the seam, not
    yet a worktree), rig-commit discovery, staging/commit, failure evidence capture +
    revert, marker reconciliation, and containment; the engine issues ZERO git commands.
    `CompletionRecorder` owns the ONE event ordering (result then integrated). Per-node
    worktree isolation later replaces the shared-workdir revert/clean policy with
    discard-the-worktree.

34. **Pass-3 defects, fixed inside 32/33.** (1) reconciliation requires the marked commit
    reachable from HEAD (§5 entry 21). (2) a failed rig's OWN commits are reverted to the
    lease's pre-HEAD after capturing them as evidence (shared-workdir policy; per-worktree
    isolation later makes this discard-the-worktree). (3) failure evidence includes
    untracked AND ignored artifacts (`git status --ignored`), and cleanup removes them
    (`git clean -fdxq`). (4) a multi-commit rig run records every commit verifiably (§3).
    (5) `ctx` file resolution rejects a symlink alias resolving into the git admin dir —
    same path-safety home (`WorkspaceEffects`) as `inplace` edit resolution.

35. **Deviation (bounded item-3 scope).** `Result.files` is NOT fully divorced from the
    effect paths this hand: in the shared-workdir PoC the committed diff IS the do's
    artifact, and the loop-artifact + resume-durability model depend on that coincidence
    (none of which are the five pass-3 defects). The receipt is the separate ownership
    record and durability predicate; the full artifact/effect divorce lands with per-node
    worktrees (step 4), when the two genuinely diverge. Event-shape versioning is a
    per-model compatibility reader, not yet an explicit schema integer (§6).

### Hand-8 calls (from the pass-4 exit review) pending review

36. **Provenance-based recovery replaces the marker scan (RECEIPT-TEAR).** `dispatched`
    carries `pre_head` — the workdir HEAD when the attempt opened (nullable; absent on a
    pre-v1 line). On resume, a top-level `do`/`inplace` that was dispatched but is not
    durable is recovered from its OWN commit range `pre_head..HEAD`: those commits are
    exactly that attempt's, so the full receipt (every rig + core commit) is reconstructed
    and retro-journalled — never re-run, never a lost rig commit. This SUBSUMES entry 21's
    reachable-marker scan for the serial model; the `wf:<run>:<epoch>:<node>` marker stays
    in commit messages as FORENSIC metadata only (the scan path is deleted, zero dead code).
    **Deviation from the owner triage:** the triage prescribed flipping the recorder to
    integrated-then-result; that ordering makes a torn tail indistinguishable from a legacy
    order AND breaks the loop partial-iteration resume floor (B4), which keys off
    result/integrated presence separately. The completion order is KEPT result-then-
    integrated; provenance recovery covers the torn window in BOTH directions (result
    without integrated → journal the receipt; nothing past dispatched → journal result +
    receipt), which is the finding's real requirement (crash-recoverable receipts) with no
    collateral damage.
37. **Lease-scoped failure transaction (FAILURE-TRANSACTION; historical, superseded by
    hand-12 entry 59).** (a) At this point a git failure integrating a successful rig's
    dirty state routed through the then-current `finalize_failure` (revert + capture),
    never a bare `ok=False`; `recover_lease` now owns that route. (b) The `Lease` snapshots the untracked +
    ignored file set at open; failure cleanup removes ONLY paths absent from that snapshot
    (recomputed AFTER the index reset, so a staged-then-unstaged leak is swept), so
    pre-existing user files survive. **SUPERSEDED by hand-11 entry 56:** a run_dir inside
    this unsandboxed shared workdir cannot be made authoritative by path exclusion alone
    and is now rejected before journal creation. (c) Nested Git
    repositories are captured (their file listing + contents, never a bare `<unreadable>`)
    and removed recursively (a plain `git clean -fd` refuses them).
38. **Explicit loop body-outcome reference (LOOP-OUTCOME-REFERENCE).** `loop_iter` carries
    `body_result_seq` — the journal seq of the body leaf's `ResultEvent`. The projection
    folds the iteration's body artifact through THAT reference, not the process-global
    last-folded result (which was only coincidentally right under serial in-order dispatch
    and wrong the moment a positional `Dispatch` completes out of order). A legacy line
    (no reference) falls back to the last result before it — the documented old-journal
    semantics, scoped strictly to compatibility, not the live fold. An empty composite loop
    body (no executable `do`/`inplace` leaf) is rejected at admission.
39. **Pre-v1 journal policy (LEGACY-COMPLETION-TAIL).** Journals are declared pre-v1 and
    unstable. A COMPLETE legacy history still folds via the compatibility readers (the
    frozen fixtures prove it), but `Journal.load` raises `JournalCompatibilityError` on an
    INTERRUPTED legacy tail — a pre-v1 event shape (a `dispatched` with no `pre_head`, a
    single-commit `integrated`, a `loop_iter` with `body_*`, an `outcome`-less `result`)
    appearing after the last boundary, which cannot be provenance-recovered. Load also
    refuses a reordered/duplicated seq stream (the floors trust seq); the append seq is
    derived from the last event's seq, not the list length, so a gap-truncated resume never
    collides. `Integrated` requires a non-empty receipt and rejects a `commit` that
    contradicts `commits` (MALFORMED-RECEIPT).
40. **Upstream ctx refs (ADMISSION-REFERENCE).** A `CtxRef(kind="node")` must be UPSTREAM:
    admission rejects a self-ref, a forward ref (pre-order position ≥ the referring node),
    and a ref that crosses a `Dispatch` (concurrent siblings, non-deterministic completion);
    a ref into an elder `Seq` sibling is fine. An `inplace` edit path that escapes only via
    a symlink (uncatchable at admission) becomes a durable FAILED result at write time,
    never an exception escaping after `dispatched`.

### Hand-9 calls (from the pass-5 exit review) pending review

41. **Two-boundary provenance model — `pre_head..post_head` (PROVENANCE-RANGE).** The
    completion certificate is a SECOND durable boundary: the `result` event gains
    `post_head`, the workdir HEAD at the moment the rig returned and the result was recorded
    (nullable only on a pre-v1 line or an unborn repo). Receipt reconstruction on resume uses
    EXACTLY `pre_head..post_head` — never `..HEAD` — so an operator commit made after process
    death sits above `post_head` and is out of range by construction, never misattributed.
    The ONLY torn window recoverable as success is a durable OK `result` whose `integrated`
    was lost (reconstruct the receipt from the two bounds). A **dispatched-only tail** (no
    `result`) has NO completion proof — a mid-rig checkpoint commit is not a certificate — so
    it is NEVER blessed as success: the resume path treats it as an incomplete attempt, runs
    the standard failure cleanup for its leftover DIRT (`git reset --hard HEAD` + untracked
    sweep, preserving committed history), and RE-RUNS the node. **Forensic-residue policy:**
    mid-rig checkpoint commits from a dead attempt remain in history UNJOURNALLED — reachable,
    harmless, and identifiable via `pre_head` lineage — and are NOT silently reset away (an
    operator commit may sit above them). This SUPERSEDES hand-8's `pre_head..HEAD` recovery of
    a dispatched-only tail (which could bless a mid-rig commit or grow after death).
42. **Clean-worktree failure lease (FAILURE-LEASE).** (a) A lease REFUSES to open on a dirty
    tracked/index worktree state — a durable failed result ("workdir has uncommitted tracked
    changes"), never proceed. This removes the reset-`--hard` destruction class: pre-existing
    tracked/staged user work can no longer exist at open, so failure revert (`reset --hard
    pre_head`) only ever undoes THIS attempt's own effects. It is the honest serial-M1 rule;
    M3 per-node worktree isolation makes the workdir engine-owned and retires the precondition.
    (SUPERSEDES hand-4's "inplace preserves a pre-existing staged index" — a dirty index now
    refuses the lease.) (b) The untracked snapshot uses per-file listing
    (`--untracked-files=all`), so an addition UNDER a pre-existing untracked directory is a
    distinct entry the lease-scoped sweep detects and removes while the pre-existing sibling
    survives. (c) Failure capture tolerates BINARY content: a decode failure records a binary
    artifact (size + sha256) instead of raising, so no exception escapes after `dispatched`.
43. **Loop outcome totality + nested-loop resume floor (LOOP-OUTCOME-TOTALITY).** (a) Admission
    rejects any composite whose LAST positional child chain does not terminate in a
    result-producing leaf (do/inplace) or a `loop` — making `ExecutionOutcome.result_key()`
    total by construction, so an uninterrupted fold and a resumed fold always agree. (b) The
    nested-loop floor bug is fixed: a FRESH loop iteration runs its body with floor `None`
    (not `-1`), and loop resume is FLOOR-SCOPED — only `loop_iter` events with seq > the
    resume floor belong to the current invocation (`projection.loop_resume`), so a nested
    inner loop is scoped to its CURRENT outer iteration and never reuses a prior outer
    iteration's inner iterations/result. The loop's own final-result short-circuit is likewise
    floor-scoped (`result_seq > floor`). (c) The `loop_iter.body_result_seq=None` live-path
    fallback is killed: totality guarantees the body always has a journalled result, so the
    live engine always emits the explicit reference; the `None` fallback in the projection is
    legacy-journal-only.
44. **Transactional inplace (INPLACE-TRANSACTIONAL).** `_exec_inplace` records every path it
    writes and the content that pre-existed (None if created). On ANY failure after the first
    write — a late symlink rejection OR a failed declared commit — it ROLLS BACK
    (`workspace.rollback_inplace`): pre-existing files are restored, created files deleted,
    the declared paths unstaged — BEFORE journaling the durable failed result. No partial
    effect survives a failed inplace.
45. **Strict seq contiguity + receipt SHA validation (SEQ+RECEIPT).** `Journal.load` requires
    EXACT contiguity: each physical event's seq is previous+1, starting at 0; negatives are
    rejected. Terra proved a torn TAIL cannot create a middle gap (append reuses `last_seq+1`
    after dropping the final partial record), so the dropped gap allowance only hid missing
    MIDDLE durability events — a middle gap now raises `JournalCompatibilityError`. A
    `CommitReceipt` rejects an empty/blank `sha` (modern `commits=[{"sha":""}]` and migrated
    legacy `commit:""` both), so an empty receipt can never mark an effect durable.
46. **Result-producing ctx refs (ADMISSION-REF-RESULTFUL).** A `CtxRef(kind="node")` must
    target a node that PRODUCES a result — an executable leaf (do/inplace) or a `loop`.
    Admission rejects a ref to a structural `seq`/`dispatch` (which journals no node-level
    result) and to an unfinished ANCESTOR composite (its result cannot exist before the
    consumer inside it runs), instead of leaving it to fail as an unresolved ctx at run time.

### Hand-10 calls (from the pass-6 exit review) — THE TRANSACTION MODEL OF RECORD

Pass 6 (three converging review passes) found ONE root cause under all remaining data-loss
and false-durability rows: **recovery state lived in process memory, and cleanup destroyed
content with unchecked git ops.** Hand-10 eliminates the class with two principles rather
than patching each row. Both are the transaction model of record — not a later refinement.

47. **PRINCIPLE A — QUARANTINE, NEVER DESTROY.** No cleanup path may delete or reset-away
    content irrecoverably.
    - **Dead-attempt recovery (dispatched-only tail).** SUPERSEDES hand-9's `reset --hard
      HEAD` + sweep. The current tip (dead-attempt commits AND any post-crash operator
      commit) is moved to an encoded, append-only quarantine ref under
      `refs/wildflows/quarantine/` (tip-SHA form specified by hand-11 entry 51), so all of
      it stays reachable; uncommitted dirt + non-preexisting untracked
      leaks are captured to `run_dir/quarantine/`; the branch is reset to the DURABLE
      `pre_head` (from the lease record, §48); only NON-preexisting untracked leaks are then
      swept (the lease's per-file `preexisting` snapshot is left in place). This kills BOTH
      pass-6 blocker rows at once: no user file is destroyed (operator commits → quarantine
      ref; pre-existing untracked → left in place), AND the *idempotent-rerun-absorbs-a-
      retained-commit* false success is impossible, because the rerun starts from `pre_head`
      and must produce its OWN receipt for any active effect.
    - **Checked live failure/rollback (mechanism superseded by hand-12 entry 59).** The
      then-current `finalize_failure` kept its revert; now `recover_lease` owns it. EVERY Git
      op in ANY cleanup/rollback path (reset, update-ref, clean, unstage) is CHECKED; a
      failure raises a typed `WorkspaceFault`. The engine then records a failed result
      explicitly marked **`workspace_unclean=True`** and re-raises to HALT the epoch (no
      `boundary(closed)` for a workspace it could not clean). A durable "failed" that lies
      the live effect was reverted is worse than a crash.
    - **`post_head=None` on an effectful result is refused.** A `result` with non-empty
      declared `files` and no `post_head` completion certificate is classified as an
      interrupted pre-v1 tail (`JournalCompatibilityError`), never accepted as a receipt-
      less durable success. `post_head` is sampled from `head_commit()` on every modern
      result; a loop's final result (non-None `loop_status`) is exempt (its durability rides
      on the body iterations' own `integrated` events). Ledger note on the unsafe interval:
      the rig-return-through-core-commit window is now SAFE — a crash there leaves a
      dispatched-only tail, which quarantine+reset re-runs to its own receipt.
48. **PRINCIPLE B — DURABLE TRANSACTION INTENTS.** Any state a restart needs to finish or
    reverse an interrupted transaction is fsynced to `run_dir` BEFORE the first mutation.
    - **Lease record.** `(pre_head, per-file preexisting untracked/ignored snapshot,
      attempt, timestamp)` is fsynced to `run_dir/leases/<epoch>-<node>-<attempt>.json` at
      lease open. Restart cleanup loads it — never process memory — so the reviewer's
      old-user-file-vs-dead-attempt-leak distinction survives process death. Cleanup is
      idempotent given the record: quarantine-ref creation, reset-to-`pre_head`, and
      capture-append all redo safely after a crash mid-cleanup. A pre-hand-10 dispatched
      line with no record falls back to a conservative journal-`pre_head` quarantine that
      NEVER sweeps (treats all current untracked as preexisting).
    - **Inplace intent.** Before the first write, every target path + whether it pre-existed
      + its original content is fsynced to `run_dir/intents/<epoch>-<node>-<attempt>.json`.
      Writes proceed; on ANY exception (a broad `except Exception`, not just `ValueError`)
      the engine rolls back from the record and journals the durable failed result; a crash
      at any point → restart finds the intent record with no matching durable result and
      reverses it BEFORE anything else (restoring even a pre-existing untracked file's
      content, which a `reset` cannot). Rollback ops are CHECKED (Principle A). The record is
      settled (removed) only AFTER the result (+ integrated) are journalled.
    - **Resume dirty check before any reset.** The resume path no longer blind-resets: it
      quarantines/captures anything not explained by the lease record, never silently
      discarding it.

    **Deviations.** (1) The `attempt` index (the node's `dispatch_count` at lease open) keys
    the lease/intent records AND the quarantine ref, so repeated dead attempts never clobber
    one another's forensics — a small addition beyond the triage's bare sketch, required for
    idempotent multi-crash recovery. (2) `workspace_unclean` was chosen (a marked failed
    result) over a separate run-level fault event, per the triage's "choose one": it keeps
    the honesty signal on the exact node whose effect survived, with zero new event kind.
    Net: the serial-M1 restart matrix has no remaining data-loss or false-durability row.

### Hand-11 calls (from the pass-7 correctness review) — TRANSACTION HARDENING

49. **Persistent unclean recovery (hand-11).** `workspace_unclean` and its explicit
    `recovery_action` (`fail|retry`) are folded into `NodeProjection`; an in-scope marker
    makes `resume_action` return `recover`, never `done`. Resume validates BOTH durable
    lease and intent records before mutation, retries intent reversal plus the checked
    quarantine/reset/sweep, and appends an explicit clean result only after every
    postcondition succeeds. `fail` then closes the already-failed attempt; `retry` remains
    a durable non-terminal rerun-pending state until a new dispatch produces its own
    result, including across a crash between cleanup and redispatch. A legacy unclean
    marker with no disposition fails closed for manual repair rather than guessing.

50. **One canonical inplace target model (hand-11).** Every edit is fully resolved at
    plan time and converted back to one workdir-relative canonical target. Intent capture,
    write, rollback/unstage, staging, commit, result files, and receipt paths all use that
    target. Two declarations resolving to the same target are rejected before the first
    write. Internal symlinks therefore edit and receipt their resolved tracked target;
    escapes/gitdir aliases remain rejected. Multiply-linked regular files are rejected:
    inode aliases (including outside-workdir hard links) have no sound single pathname to
    stage/receipt. Modern intents preserve original bytes as base64 and the exact UTF-8
    bytes the attempt expected to write.

51. **Append-only quarantine histories (hand-11).** A quarantine ref is immutable per
    observed tip: its name includes the full tip SHA and creation uses compare-and-create.
    Dead attempts, operator tips observed on redo, and failed rigs that authored commits
    are all preserved before reset; a redo allocates a second ref rather than moving the
    first. Run/node ref components use only `[A-Za-z0-9._-]`; lossy, empty, `.lock`,
    dot-edge, and overlong forms receive a SHA-256 suffix. Thus arbitrary filesystem-valid
    run-directory names cannot make Git recovery fail solely through ref syntax.

52. **Checked filesystem cleanup (hand-11).** Cleanup has no `ignore_errors`: status/diff
    enumeration, directory traversal, byte reads, capture publication, unlink/rmtree, and
    Git reset/ref operations are checked. Every removal has an `lexists` absence
    postcondition. The lease additionally snapshots pre-existing directories, allowing
    attempt-created empty directory roots to be captured/removed and pre-existing empty
    directories to be recreated if an attempt deletes them. Any incomplete Git
    status warning or filesystem failure raises `WorkspaceFault` and enters decision 49's
    persistent halt.

53. **Byte-recoverable immutable capture (hand-11).** Before any destructive reset or
    sweep, tracked current files, untracked/ignored files, symlinks, empty directories,
    and recursively enumerated nested-repository contents are copied exactly into a unique
    `.capture/` directory. `manifest.json` records path/kind/size/SHA-256/blob; raw blobs,
    the manifest, and the human-readable binary Git patch/index are fsynced. Lease open
    also captures exact baseline bytes for pre-existing untracked/ignored objects; cleanup
    captures their current value, then restores the baseline, so overwrite/deletion cannot
    be absorbed by a retry. Success refuses a rig-authored commit that claims such a path,
    fails if its baseline changed, and excludes an unchanged user path from core staging.
    Retry suffixes make captures append-only. A hash summary is evidence, never the only
    retained copy.

54. **Divergent intent reversal (hand-11).** Rollback first compares each canonical live
    target with BOTH the expected attempt bytes and the recorded pre-state. A third state
    (including binary post-crash operator bytes) is durably captured through decision 53
    under `intent-reversal/` before restoration. Parent directories absent at intent time
    are also recorded and removed on rollback; unexpected contents are captured first. A
    changed canonical path topology (for example, an operator-installed parent symlink)
    halts without overwriting it.

55. **Atomic durable-record lifecycle (hand-11).** Lease/intent and capture-manifest
    publication is same-directory temp write → file fsync → `os.replace` → parent-directory
    fsync. Record unlink is followed by parent-directory fsync. Newly created run-directory
    components and the first `events.ndjson` entry are parent-directory-fsynced before any
    lease mutation. Records carry a canonical SHA-256 integrity digest in addition to
    identity/provenance checks, so schema-valid
    field corruption cannot accidentally reclassify user files or add rollback targets
    (the digest is corruption detection, not hostile-writer authentication). A present
    unsigned, non-regular, unreadable, malformed, integrity-invalid, or schema-invalid
    record is a typed `WorkspaceFault`; it is never confused with an absent legacy record
    and recovery performs no mutation after such a load failure.

56. **Pass-7 crash proof (hand-11).** The transaction regressions use real `fork` plus
    `os._exit` at the record-publication, inplace-write, rig-death, quarantine-reset, and
    cleanup-before-redispatch windows. No deviation from the pass-7 intended directions:
    canonical symlink support was chosen over blanket rejection because the resolved path
    can be used consistently by every transaction operation in the serial-M1 model;
    hard-linked targets are rejected because pathname resolution cannot canonicalize an
    inode alias. **Evidence-backed deviation:** the serial shared-workdir engine rejects a
    `run_dir` inside `workdir` before creating the journal. An unsandboxed rig has the same
    OS identity and can unlink/rewrite/commit any in-workdir exclusion, so hand-8 entry 37's
    claimed hard exclusion was unsound; target-repo `.wildflows/` run state waits for the
    per-node worktree authority boundary.

57. **Active completion certificate (hand-11).** A successful rig's `post_head` must
    descend from its lease `pre_head`; backward/unrelated history movement is a failed
    transaction and cleanup restores the original tip. Torn result→integrated recovery
    additionally requires `post_head` to be an ancestor of live `HEAD` AND every receipted
    path to match its `post_head` tree value. If an operator reset/diverged or committed a
    descendant revert, both attempt and operator tips are quarantined, the lease baseline
    is restored, a durable retry is journalled, and the node reruns. A receipt is never
    reconstructed for an effect absent from the active tree. Result events explicitly mark
    `receipt_required`, so even an active `--allow-empty` commit (zero changed paths) cannot
    become durable in the result→integrated tear without its history receipt. An interrupted
    pre-field result is refused as a legacy tail because absence cannot distinguish an
    effectless result from that empty-commit crash window.

58. **Recovery topology containment (hand-11).** Baseline capture/restore validates every
    intermediate parent against its canonical workdir location before reading or deleting.
    It preflights every manifest path and verifies every raw blob before the first deletion.
    A post-crash parent-symlink substitution or corrupt late blob therefore halts without
    losing live pre-existing bytes or following a path into an external directory.

### Hand-12 calls (from the pass-8 correctness review) — ONE RECOVERY TRANSACTION

59. **One lease-recovery transaction (hand-12, structural spine).** The duplicated live-
    failure and dead-attempt cleanup state machines are deleted. `WorkspaceEffects.recover_lease`
    is the sole destructive recovery authority; live do/inplace failure, dispatched-only
    restart, inactive-certificate recovery, and persistent-unclean resume all submit a
    `RecoveryRequest`. Its explicit phases are: **validate every durable record and
    provenance input → capture every destructible byte → append-only quarantine every
    observed/certified tip → reverse intent/reset exact committed-or-unborn state/sweep →
    restore the lease baseline → VERIFY COMPLETE POSTCONDITIONS → publish a recovery
    receipt and settle lease/intent → let the engine publish the halt-clear/final Result**.
    Engine-side lease/intent loads, reversal decisions, and alternate preserve/reset paths
    are gone; non-destructive Git/path primitives remain shared helpers, not state machines.

60. **End-state proof, not command success (B1).** Recovery verifies exact `HEAD == pre_head`
    (or both unborn), byte-mode clean tracked/index status, an empty lease-scoped attempt
    leak set (including created empty directories), and byte-equivalence of every modern
    pre-existing baseline before it can settle or clear. A successful `reset --hard` whose
    non-invertible smudge filter leaves tracked dirt therefore becomes a persistent
    `workspace_unclean` halt. Legacy no-record recovery remains conservative/no-sweep and
    cannot claim a baseline it never recorded; this exception is available only when the
    dispatch explicitly lacks the modern required-lease marker.

61. **Reversal alias and portable case policy (B2/H5).** Intent validation re-stats every
    existing canonical target immediately before expected/pre-state classification. A
    regular file with `st_nlink != 1` is byte-captured and raises `WorkspaceFault` before
    any overwrite or unlink. Each path fsyncs `started=True` before its first write and a
    per-path `reversed=True` after restoration; absent-prestate targets stay linked until
    the transaction's checked leak sweep, avoiding an unlink-before-progress crash gap.
    The intent publishes its aggregate `reversed=True` after reversal/unstage and `swept=True`
    from inside the checked sweep's completion boundary. A started canonical target that
    disappears before that durable sweep boundary still fails closed because
    a hidden external alias may retain attempt bytes. Initial planning uses `(st_dev,
    st_ino)` identity for
    existing targets and an
    NFC+casefold canonical key for all declarations. **Documented conservative deviation:**
    case-canonical declaration collisions are rejected on every filesystem, not only after
    proving the workdir case-insensitive. A reliable absent-name case-sensitivity probe
    would itself mutate the unleased workspace and platform case/normalization rules are
    not uniform; portable deterministic rejection is narrower and fail-safe.

62. **Checked, byte-safe, object-format-aware Git reads (B3/H1/H4).** Every `rev-list` and
    `diff-tree` receipt read is checked; launch, nonzero, and decode failures become
    `WorkspaceFault` and enter the same recovery transaction, never an effectless or
    ownership-empty success. All `-z` pathname plumbing runs in bytes mode. UTF-8 paths
    retain their ordinary wire spelling; arbitrary POSIX bytes use an escaped reserved
    base64 prefix (the prefix itself is escaped), consistently across leases, intents,
    manifests, Results, and receipts. Every Git boundary treats decoded owner/Git-derived
    names as literal pathspecs, so a filename such as `:(glob)**` cannot widen ownership.
    Repository object format is read with checked
    `rev-parse --show-object-format`; null OID width and empty-tree OID are selected for
    SHA-1 or SHA-256 rather than hard-coded to SHA-1.

63. **Required modern records and settle-before-clear (H2/H3).** New `Dispatched` events
    carry `lease_required=True`; a planned non-empty inplace also carries
    `intent_required=True` after publishing its intent and before the dispatch. Missing
    required records fail closed; a missing current lease with neither a matching immutable
    settled-lease certificate nor recovery receipt fails closed. No global schema integer
    is needed: absence of the additive marker identifies the conservative legacy path.
    After postcondition verification, recovery writes an integrity-bound, create-once
    `recoveries/<attempt>.json` containing the complete lease and deterministic fail/retry
    Result, then archives the signed lease and unlinks/fsyncs lease+intent, and only then
    returns for Result publication. A settlement failure leaves the prior unclean marker;
    death after settlement but before Result is completed from the retained receipt after
    re-verifying final state. Recovery and settled-lease receipts are retained as immutable
    settlement certificates; garbage collection is a separate future transaction. This
    supersedes hand-11 entry 55's result-before-record-unlink ordering for recovery paths.

64. **Uniform capture integrity (H6).** Baseline, failed-diff, quarantine, and intent-
    reversal manifests all carry a canonical outer SHA-256 digest. One checked loader
    validates manifest identity/path topology plus every file blob's size and SHA-256;
    baseline restoration and public forensic round trips share it. Atomic publication
    remains the torn-write defense; the digest/blob checks add post-publication corruption
    detection. No pass-8 intended direction was otherwise changed.

### Hand-13 calls (from the pass-9 correctness review) — CLOSE THE TWO TEARS

65. **Per-path sweep proof (hand-13).** Every absent-prestate inplace target receives a
    signed `IntentWrite.swept=True` update, fsynced immediately before the checked leak
    unlink that covers that path. Restart accepts a disappeared started target only when
    this per-path proof (or the legacy aggregate completed-sweep proof) is durable; an
    unmarked disappearance remains the hidden-hard-link ambiguity and fails closed. Sweep
    preparation revalidates target topology/link counts before publishing each proof, and
    directory leak roots mark every intent target they contain. The aggregate `swept`
    marker remains the all-path completion boundary, not the only redo classifier.

66. **Required-record settlement before torn integration (hand-13).** A torn OK
    `result` may reconstruct `pre_head..post_head`, but before publishing its recovered
    `Integrated` it must validate the dispatch's required lease/intent records (or an
    immutable settled/recovery replacement). It then publishes an integrity-bound,
    create-once completion-settlement certificate containing those validated records and
    settles the active records *before* `Integrated`. A crash during settlement or after
    settlement but before integration reuses that certificate and safely redoes both;
    missing marked records with no replacement raise `WorkspaceFault` and leave the epoch
    open. This extends hand-12 H3's settle-before-publication rule to the success tear.

### Hand-14 calls (from the pass-10 correctness review) — CLOSE THE EFFECT CHANNEL

67. **Verified read-only predicates (hand-14).** Every command `until` runs under a
    required durable lease keyed by the loop's internal `<node_id>.until` evaluation node.
    The standard recovery postcondition proves exact HEAD, clean tracked/index state, no
    new untracked/ignored paths or directories, and byte-identical pre-existing baselines
    before accepting the exit code. Any mutation or unverifiable postcondition runs the
    one recovery transaction (capture, revert, verify, settle), records a durable failed
    evaluation, raises `PredicateEvaluationError`, and leaves the epoch open. Predicate
    host/service effects remain outside engine mediation just like rig-command external
    effects, so the command contract is an **idempotent check**. Its exact redo interval is
    **predicate-executed → matching `loop_iter` durable**: restart recovers the unfinished
    evaluation lease and reruns the predicate, while a completed body remains skipped.
    This reverses entry 8's historical allowance for untracked predicate artifacts.

68. **Append-owner poisoning (hand-14).** `Journal.append` computes/serializes the assigned
    sequence without changing live state, then mutates `_events` and the projection only
    after write+flush+fsync durability succeeds. Any durability exception poisons that
    owner; subsequent appends raise typed `JournalPoisonedError` until the caller performs
    a fresh `Journal.load`, which alone decides whether the uncertain tail is complete or
    torn and therefore which contiguous sequence comes next. M2's central append owner
    must replace a poisoned instance rather than reuse or retry through it.
