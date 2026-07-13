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
  by the core regardless of what the body claims.

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
- **Failure modes:** a write outside the workdir is rejected (path-escape guard); a
  commit with nothing staged is a no-op result with `ok=False`.

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
| `boundary`   | an epoch opens / closes                   | `phase: opened\|closed`, `expr` (opened: the admitted tree), `rails` |
| `dispatched` | a `do`/`combine`/`inplace`/`setup` starts | `rig`, `task`/`cmd`, `workdir` |
| `result`     | an agent/primitive produced output        | `ok`, `text` tail, `files`, `exit_code?`, `loop_status?` |
| `integrated` | the core applied+committed a result       | `commit`, `paths` (disjoint-ownership set) |
| `judged`     | a `do`-as-judge produced a verdict         | `verdict`, `ok`, `target_node` |
| `loop_iter`  | a `loop` completed one iteration          | `iteration`, `commit`, `converged` |
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
  marker over them, not a replacement.
- `integrated` is emitted only by the **core**, never a rig — it is the mediation
  proof (invariant 1). This holds for **`do`** too (hand-4, B5): after a rig runs, the
  core stages + commits the worktree's changes (`git add -A -- .`, message keyed by
  node_id) and emits `integrated` with the changed paths. An effectless `do` (no diff)
  legitimately produces no `integrated`.
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
nor enforces one it never admitted. Until then, **the only live rail is the executable
`loop`'s `cap`** — a `loop` cannot exceed `node.cap` (core-enforced; cap-exhaustion is
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
- **`inplace`:** `integrated` present → done (the commit is durable); `dispatched`
  without `integrated` → re-apply the edits (whole-file writes are idempotent). An
  empty/no-diff inplace is a durable no-op.
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
  iteration re-runs its whole body (floor = +∞).
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
the *only* durable run state the dashboard and resume consume — everything else (git
tip, worktree artifacts) hangs off node_ids recorded in it. The journal exposes:
`append(event) -> seq`, `events() -> list[Event]`, and `load(run_dir) -> Journal`.

**Torn-tail tolerance (B2, hand-4):** a kill/power-loss during the final `write()` can
leave the last ndjson line unterminated or malformed. `load` drops exactly that one
torn FINAL record (it never durably completed, so the next append reuses its seq) and
still raises on any malformed COMPLETE or MIDDLE record. fsync-on-append bounds the
damage to the last line; it does not eliminate a partially written last record.

**Single-writer precondition (N2, hand-4):** the journal derives `seq` from its
in-memory length and has **no lock or writer queue**. `Engine`-load-continues-seq (B1)
covers **serial restarts only** — one append owner at a time. Parallel dispatch (ladder
step 3) MUST introduce a single central append owner or an interprocess lock before any
concurrent children/processes append; two live `Journal` instances over one run_dir
would emit duplicate/reordered seqs. Not built this hand.

---

## 7. Worktree mediation & the disjoint-ownership merge (invariant 1)

Every `do`/`dispatch` child runs in its **own git worktree** off the run's base commit.
The core — never the model — integrates results by applying each child's diff and
committing, enforcing **disjoint ownership**: two integrated siblings may not both
modify the same path (a collision fails the integration and surfaces to the planner,
who re-shapes). `inplace` and `setup` are the exceptions: `inplace` commits directly in
the workdir (it is the planner's hands, serialized), `setup` mutates host state by
design. Worktree lifecycle hardening (creation, disjoint merge, reap-on-kill, cleanup)
is ladder step 4; the PoC (step 1) runs `do`/`inplace` in a single workdir to prove the
mind-steers loop end-to-end, and records the seam (`Rig.run(prompt, workdir)`) that the
worktree layer slots behind.

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

Build order (D7/D8, bottom-up): **(1) PoC** — `do`+`inplace`, in-memory, one rig, prove
the mind-steers loop on a toy task ← *this build*. (2) durability — journal file writes,
resume-from-journal. (3) composition — `dispatch`/`combine`/`loop` + rails. (4) worktree
hygiene. (5) `.wildflows/` target-repo folder (config, skills, run state, setup seam).
(6) dashboard.

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
   working tree (`git status --porcelain`). A loop's `until` predicate legitimately
   leaves untracked iteration artifacts in the workdir; the old whole-tree check would
   force a commit with nothing staged (git error). Staged-only is the correct
   "did the declared edits change anything" test and keeps `inplace` re-apply idempotent.

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
