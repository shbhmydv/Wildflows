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
disjoint by construction and run through a bounded executor. Every sibling in one
concurrent leaf group starts from the same run-tip snapshot; completion and integration
order are implementation details, never a contract. When execution order matters, wrap
the steps in `seq`, not `dispatch`.
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
- **Failure modes:** as `do`; an input without a successful durable result raises typed
  `CombineDependencyError` before the combiner starts. `combine` reads inputs from the
  journal/artifacts, so resume reuses durable upstream results, never live memory.

### `loop(expr, until, cap)`
Repeat a sub-expression until a condition holds or a cap is hit. The body **may** hold
a live session across iterations (the senior→junior pattern: a senior keeps context,
juniors fan out, the senior picks up junior failures).
- **Inputs:** `body` (an expression), `until` (a checkable condition — a `setup`-style
  command whose exit code is the predicate, or a planner-judged result flag), `cap`
  (max iterations — a rail).
- **Output:** the result of the last integrated iteration.
- **Failure modes / D5 (SETTLED):** live loop-session state dies with the process. On
  resume the loop restarts at its last `loop_iter` floor; no session serialization. The
  core enforces `cap`. A command predicate runs with a timeout in its own fresh detached
  throwaway worktree at the run-branch tip. Its exit code is accepted and the whole
  worktree is discarded, so tracked/index/untracked mutation has no accepted effect. A
  crash before `loop_iter` reruns the check in another never-reused worktree.

### `inplace(edit)`
The planner's own hands — a small fix with no sub-agent.
- **Inputs:** an `edit` = a set of `(path, content)` writes the planner authored
  directly (whole-file content; **(hand-1 call, review pending)** — a unified-diff /
  patch form is a later addition, whole-file write is the minimal core).
- **Output:** a `Result` describing what was written; `ok` reflects whether the core's
  commit succeeded.
- **Effects:** the core writes and commits only the declared paths in a fresh node
  worktree, validates the resulting receipt, then fast-forwards the run branch.
- **Failure modes:** an escape, undeclared changed path, write/commit error, or failed
  fast-forward abandons the worktree and leaves the run branch untouched. An identical
  edit with no commit is a durable no-op.

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
point (§3). The tree is validated by Pydantic on admission; all eight kinds are
executable except a `loop` with a planner-judged `flag` predicate.

### Code homes (hand-6 authority split, from the bloat audit)

The engine was split by **authority**, each home owning one thing so the planned
scheduler / worktree / planner / dashboard steps each depend on one narrow seam:

| Home | Sole authority |
|---|---|
| `engine.py` | epoch lifecycle, expression traversal, primitive orchestration |
| `admission.py` | dealias + deterministic ids + whole-tree validation (`admit_epoch`) |
| `projection.py` | the one live journal fold (`RunProjection.apply`), resume decisions, `ExecutionOutcome`, `replay` |
| `workspace.py` | plain Git authority: fresh worktrees, receipts, containment, fast-forward CAS, best-effort removal |
| `result.py` | the `Result` / `Outcome` artifact value + `CommitReceipt` / `IntegrationReceipt` effect record |
| `journal.py` | the single append owner: seq-assign + fsync + `projection.apply` |
| `planner.py` | hard planner-decision/rails models and typed parked states |
| `run.py` | planner prompt/digest, decision durability, rails, and epoch run loop |

There is exactly ONE state system: the journal's live `RunProjection`, folded by one
`apply(event)`. Load replays the ndjson through the same `apply`, so a running
projection and a reloaded one are bit-identical (covered by journal and resume scenarios).
Resume/durability is one decision — `resume_action` — over
an explicit attempt/iteration `floor` (`-1` top-level, an int for a resumed-partial loop
iteration, `None` for a fresh one) that replaces the old `_NO_RESUME` sentinel.

### Admission (hand-6) — one pass before `boundary(opened)`

`admission.admit_epoch(tree, epoch, projection, registry)` runs BEFORE any execution
event: it dealiases (wire round-trip), assigns deterministic node ids, then makes ONE
whole-tree traversal to reject a plan the core can refuse **without effects**. On
rejection it raises **`AdmissionError`** (a `ValueError` subclass) and NO journal event
is written — a representable-but-unrunnable shape never opens an incomplete epoch, so
`NotImplementedError` after a durable boundary is gone. Admission rejects:

- **executor capability** — non-root `setup.cwd` and `loop` with a `flag` predicate
  (representable, not yet executable);
- **unknown rig names** — every rig-bearing node's `rig.name` must resolve in the registry;
- **ctx node refs** — a `CtxRef(kind="node")` must name a node in the epoch tree;
- **resume identity** — on an already-open epoch, the supplied tree must equal the
  journalled `boundary(opened).expr` (a divergent resumed tree is a caller error).

**Local Pydantic invariants** (single-model, checked at construction) carry the rest:
lexical path guards on `Edit.path` and file `CtxRef.ref` (leading dash, absolute, `..`,
literal `.git`), duplicate paths within one `Inplace`, positive `loop.cap`, and positive
rig `timeout_s`. **Environment-dependent** checks stay at use time in `Repository`
(symlink escapes, a not-yet-created ctx file, Git/process failures, the actual `until`
result) — a validator cannot resolve a symlink.

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
types (in `wildflows/events.py`), each a Pydantic model sharing a v1 header
`(version, seq, ts, run_id, epoch, node_id, kind)`:

| event        | emitted when                              | key fields |
|--------------|-------------------------------------------|------------|
| `boundary`   | an epoch opens / closes                   | `phase`, `expr`; opened pins `run_branch`, `base_commit`, typed `rails`, `rationale` |
| `dispatched` | a `do`/`combine`/`inplace`/`setup` starts | `rig`, `task`/`cmd`, unique `workdir`, `pre_head` |
| `result`     | an agent/primitive produced output        | `outcome`, `text`, `files`, `artifact`, `post_head`, `receipt_required`, `loop_status?` |
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
  the run-branch tip after the body integrated, and whether `until` converged. **(hand-2
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
- **Completion ordering: one order — `result` THEN branch fast-forward THEN
  `integrated`**, for do, inplace, and resume reconciliation. An effectful result without
  its receipt is not done: exact branch position decides whether resume reconstructs the
  landed receipt or journals a fallback and reruns.
- `integrated` is emitted only by the **core**, never a rig — it is the mediation
  proof (invariant 1). This holds for **`do`** too (hand-4, B5): after a rig runs, the
  core stages + commits the worktree's changes (`git add -A -- .`, message keyed by
  node_id) and emits `integrated`. **The `integrated` event carries an
  `IntegrationReceipt` — EVERY commit in `pre..post` (a rig may legitimately author
  several), each with its own changed paths (hand-7, item 3 / defect 4)**; `commit` (the
  final sha) and `paths` (the order-preserving union — the disjoint-ownership set) are
  derived. Replay **accumulates** a node's receipts (union of commits) — no last-write-wins
  on a single `paths` list. An effectless `do` (no diff) legitimately produces no
  `integrated`. **Result vs effect:** `Result.files` is the agent artifact list; the
  `IntegrationReceipt` is the commit/path ledger. Durability keys off
  `receipt_required` plus the later receipt, never artifact names.
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

## 4. Rails (SETTLED invariant 3)

`PlannerDecision.rails` is a strict block riding each admitted opened boundary:

```
Rails: deadline_s: float | None       # run wall-clock from durable creation
       max_epochs: int | None         # number of executable expression epochs
       budget_notes: str | None       # prompt context only; no token accounting yet
```

The planner declares deadline/max epochs on its first expression and may update them on
later re-entries. A deadline may only move downward. `Run` checks both rails before a
planner re-entry and expression open; `Engine` also checks deadline before each node, so
the policy is refuse-to-start rather than an in-flight kill. A hit raises typed
`RailStop`. An open epoch remains open with its exact durable expression, making restart
repeat the same stop without replanning or dishonestly closing unfinished work. Loop
`cap` remains the existing per-loop core rail. Token/cost accounting is deferred.

---

## 5. Resume semantics (hand-19 worktree model)

Resume is replay plus cheap Git verification. An opened epoch keeps its admitted tree;
a closed epoch is a no-op. Every opened boundary pins the run ref and its base commit.
The engine verifies each active `integrated` receipt with plain Git subprocesses: every
SHA exists, its recorded integration base through `commit` is the exact linear range,
each changed-path claim matches Git, and the live run-branch tip is exactly the newest
verified claim. The integration base defaults to `dispatched.pre_head` for serial and
legacy events; a rewritten sibling records the moving tip explicitly. A live
descendant or any other unknown tip is operator activity and raises
`BranchDivergedError`; the engine never resets an operator-owned run branch.

Leaf rules are deliberately small:

- A durable successful effectless `result` is done.
- A successful effectful `result` is done only after its later `integrated` receipt.
- `dispatched` without `result` is an interrupted attempt. A serial attempt appends the
  existing tail-invalidating fallback. An interleaved sibling whose shared base is an
  earlier verified tip gets a serialized failed result instead, preserving siblings
  already integrated later in the stream; only that non-integrated node reruns from the
  current tip in a new uniquely named worktree.
- In the `result`→branch-fast-forward→`integrated` crash window, branch at `post_head`
  reconstructs and journals the receipt; branch still at `pre_head` journals a fallback
  and reruns; any third tip is refused.
- An unverifiable receipt at an inactive tip appends one `boundary(opened)` carrying
  `fallback_from`; projection atomically discards that attempt and every dependent later
  journal fact, then reruns from the preceding verified tip. If the alleged effect is
  live, the engine refuses rather than guessing.
- A rig, timeout, path, or integration failure journals a failed result, abandons the
  attempt worktree, leaves the epoch open, and reruns on the next `run_epoch` call.

Loop replay still uses `loop_iter` floors: state at or below the last completed
iteration is stale for a partial iteration; a fresh iteration uses `floor=None` and
reruns its body. A predicate result without its matching `loop_iter` is evaluated again
in another throwaway worktree. No session serialization and no worktree recovery exist.
Leftover registered worktrees are garbage; their paths are never reused.

---

## 6. The journal (in-memory + ndjson)

The journal remains the run spine at `<run_dir>/events.ndjson`: one typed event per line,
one append owner, sequence assignment at append, flush + file `fsync`, and an in-memory
`RunProjection` updated only after durability succeeds. `load` replays through the same
fold, so running and resumed projections are identical.

An unterminated final record is uncertain and is durably truncated to the last newline;
a malformed complete/middle record raises. Sequences must be contiguous from zero.
A failed append poisons that owner; only `Journal.load` may adopt a complete residue or
repair a torn one, and load fsyncs the accepted file and directory before returning.
`Journal(run_dir)` is creation-only for a nonempty file; `Journal.load` is continuation.

Every record carries integer `version: 1`; the first record fixes the stream version.
`Journal.load` raises typed `IncompatibleJournalError` for an unversioned or non-v1
stream. There is no migration machinery. Parallel dispatch has one central append
owner: worker threads return candidates only; the coordinator alone appends results,
moves the ref, and appends integration facts.

---

## 7. Per-node worktrees and run-branch integration (invariant 1)

A run owns one existing Git branch. `Run` places `run_dir` at target-local
`.wildflows/runs/<run-id>/`; direct `Engine` callers may still choose another path. It
contains `events.ndjson`, planner attempt outputs (successful decisions verbatim),
result artifacts, and a run-scoped `worktrees/` directory.
For every `do` and `inplace`, the core:

1. verifies the run branch at the newest journalled tip;
2. creates a uniquely named detached `git worktree add` at that tip;
3. runs the rig there, or applies only the declared inplace writes there;
4. core-commits any remaining successful changes and derives the exact linear
   `IntegrationReceipt` for `pre_head..post_head`;
5. journals `result`, fast-forwards the run branch, then journals `integrated`; and
6. best-effort runs `git worktree remove --force`.

The fast-forward is `git merge --ff-only` when the supplied target worktree has the run
branch checked out (keeping its files/index current), otherwise a compare-and-swap
`git update-ref <ref> <candidate> <base>`. A serial node lands its original candidate.
For a concurrent sibling built on the group's older shared base, the sole integrator
cherry-picks its verified linear commit chain in a fresh disposable worktree at the
moving run tip, validates that the rewritten per-commit path lists are unchanged, then
fast-forwards that rewritten candidate. Conflict/sequencer state is discarded with the
throwaway tree; only the landing SHAs enter `integrated`. Before reapply, the source
receipt paths must be disjoint from every sibling already landed in this group. Exact
path overlap fails the later lander without moving the ref; a retry is a fresh attempt
from the new tip and is no longer part of the old concurrent ownership group.

A failed rig, core commit, declared-path check, or fast-forward never changes the run
branch. Its worktree is discarded. A crash may leave a registered worktree or escaped
process writing there, but new attempts use new paths and branch acceptance depends
only on verified commits. Worktree isolation is an authority boundary against accidental
writes, not a security sandbox: a same-UID rig can address the common Git directory.
Predicates use the same fresh detached worktree and discard it after the exit code; their
filesystem/index mutation has no accepted effect.

---

## 8. Rigs (the multi-harness seam)

A **rig** is a harness+model that executes a `do`. The seam is one method:

```
Rig.run(prompt: str, workdir: Path) -> Result
```

prompt in, text/files out — exactly grindstone's shape-agnostic `request.sh` contract,
which is why real rigs (`claude -p`, `pi`, local Qwen, `codex exec`) plug in with
no engine change. The core ships three Python rig types:
- **`EchoRig`** — deterministic, returns a canned/derived result; the test substrate.
- **`ShellRig`** — shells out to an arbitrary command template (e.g.
  `claude -p {prompt}` run in `{workdir}`), capturing stdout/exit as the result.
- **`ScriptRig`** — invokes the process contract below. Bundled `pi`/picodex and local
  OpenAI-compatible adapters live in `rigs/`; model calls remain operator-run.

A `RigRef(name, params)` in an expression names which rig and its config; the core
resolves it through a **rig registry** at execution time. Rigs may write or commit only
inside their detached attempt worktree; the core alone advances the run branch. Built-in
external rigs start a process group and kill it on timeout (and quiesce ordinary
background children on return). There are no durable process records or restart reaper:
a crash can leave an orphan writing only a never-reused attempt path.

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
- **Per-dispatch log dir** starts at `<log_dir>/<workdir.name>/` and gains a numeric
  suffix if that name already exists. It contains captured `agent.stdout.log` /
  `agent.stderr.log` plus the prompt, so planner retries and worker attempts never
  overwrite logs.
- **`busy` is journalled as `ResultEvent(ok=False, outcome="busy")`** — distinct from a
  failure so a later ladder step can back off + re-enter. No backoff/retry policy is
  built in this hand.
- Bundled scripts live in `rigs/` and are documented in `docs/RIGS.md`. Rigs are
  declared in owner-facing **`rigs.yaml`** (YAML by policy), a Pydantic discriminated
  union (`echo|shell|script`) loaded by `load_rigs(path) -> RigRegistry`; unknown kinds
  are rejected and relative script/log paths resolve from the YAML file.

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
(invariant 4); with no acceptance, planner judgment is the exit. This named macro is not bundled yet, but its `dispatch`/`combine` primitives and event
vocabulary are executable.

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
dashboard. **Current status:** the engine has fsynced journal replay, per-node and
predicate worktrees, exact receipt/run-tip verification, executable
`do`/`inplace`/`ask`/`setup`/`seq`/bounded-`dispatch`/command-`loop`, and a
planner-rig run loop with deadline/max-epoch rails, target-local run state, executable
`combine`, and bundled model adapters. The dashboard remains a later step.

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
8. (superseded by hand-19 worktree model) **`_commit` checks the STAGED diff only** (`git diff --cached --quiet`), not the whole
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
10. (superseded by hand-19 worktree model) **`ScriptRig` per-dispatch log dir keyed by `workdir.name`**, not an explicit
    node_id param — the `Rig.run(prompt, workdir)` protocol carries no node_id, and the
    real worktree seam (step 4) names each `do`'s worktree for its node_id, so the key
    is node_id by construction. Revisit if step 4 names worktrees differently.
11. **Timeout is represented as `outcome="failed"` + a `[timeout]` text marker**, not a
    fourth outcome — the caller treats a timeout like any other non-busy failure.
    `ScriptRig` kills the direct child only (`subprocess` timeout). **The historical
    group-reaping deferral is SUPERSEDED by hand-16 entry 73's outer supervisor.**
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
15. (superseded by hand-19 worktree model) **`inplace` commits ONLY its declared paths** (B6) via a `--`-scoped pathspec commit
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

21. (superseded by hand-19 worktree model) **`do` reconciliation rule (NB4).** Every commit the CORE makes carries a
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

22. (superseded by hand-19 worktree model) **Rig-commit recording + shared-workdir reset policy (NB5).** After every `do` the
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
    An unknown rig name is a journalled failed result, not a crash (SHOULD-FIX 2).
    **Hand-16 entry 73 supersedes ShellRig's historical self-created group/`killpg` path
    with the one outer supervisor.** A blank/whitespace `Until.cmd` is
    rejected at admission (SHOULD-FIX 5). Core integration parses changed paths NUL-
    delimited (`-z`) so whitespace filenames stay one path (SHOULD-FIX 6). A `ShellRig`
    non-zero exit sets `outcome="failed"` (SHOULD-FIX 7).

27. **N2 (single-writer journal) remains DEFERRED to ladder step 3** (§6) — documented,
    not implemented this hand: the current fixes harden the SERIAL restart/effect
    invariants that step 3's parallel dispatch would otherwise build on unsoundly.

### Hand-6 calls (from the bloat audit) — an equivalence refactor

28. (superseded by hand-19 worktree model) **Authority split (hand-6, from bloat audit).** The 700-line `Engine` was split into
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

33. (superseded by hand-19 worktree model) **Workspace effect transaction + completion recorder (item 4).** `WorkspaceEffects`
    owns the per-node-attempt lease (pre/post HEAD over the shared workdir — the seam, not
    yet a worktree), rig-commit discovery, staging/commit, failure evidence capture +
    revert, marker reconciliation, and containment; the engine issues ZERO git commands.
    `CompletionRecorder` owns the ONE event ordering (result then integrated). Per-node
    worktree isolation later replaces the shared-workdir revert/clean policy with
    discard-the-worktree.

34. (superseded by hand-19 worktree model) **Pass-3 defects, fixed inside 32/33.** (1) reconciliation requires the marked commit
    reachable from HEAD (§5 entry 21). (2) a failed rig's OWN commits are reverted to the
    lease's pre-HEAD after capturing them as evidence (shared-workdir policy; per-worktree
    isolation later makes this discard-the-worktree). (3) failure evidence includes
    untracked AND ignored artifacts (`git status --ignored`), and cleanup removes them
    (`git clean -fdxq`). (4) a multi-commit rig run records every commit verifiably (§3).
    (5) `ctx` file resolution rejects a symlink alias resolving into the git admin dir —
    same path-safety home (`WorkspaceEffects`) as `inplace` edit resolution.

35. (superseded by hand-19 worktree model) **Deviation (bounded item-3 scope).** `Result.files` is NOT fully divorced from the
    effect paths this hand: in the shared-workdir PoC the committed diff IS the do's
    artifact, and the loop-artifact + resume-durability model depend on that coincidence
    (none of which are the five pass-3 defects). The receipt is the separate ownership
    record and durability predicate; the full artifact/effect divorce lands with per-node
    worktrees (step 4), when the two genuinely diverge. Event-shape versioning is a
    per-model compatibility reader, not yet an explicit schema integer (§6).

### Hand-8 calls (from the pass-4 exit review) pending review

36. (superseded by hand-19 worktree model) **Provenance-based recovery replaces the marker scan (RECEIPT-TEAR).** `dispatched`
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
37. (superseded by hand-19 worktree model) **Lease-scoped failure transaction (FAILURE-TRANSACTION; historical, superseded by
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
39. (superseded by hand-19 worktree model) **Pre-v1 journal policy (LEGACY-COMPLETION-TAIL).** Journals are declared pre-v1 and
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

41. (superseded by hand-19 worktree model) **Two-boundary provenance model — `pre_head..post_head` (PROVENANCE-RANGE).** The
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
42. (superseded by hand-19 worktree model) **Clean-worktree failure lease (FAILURE-LEASE).** (a) A lease REFUSES to open on a dirty
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
44. (superseded by hand-19 worktree model) **Transactional inplace (INPLACE-TRANSACTIONAL).** `_exec_inplace` records every path it
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

47. (superseded by hand-19 worktree model) **PRINCIPLE A — QUARANTINE, NEVER DESTROY.** No cleanup path may delete or reset-away
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
48. (superseded by hand-19 worktree model) **PRINCIPLE B — DURABLE TRANSACTION INTENTS.** Any state a restart needs to finish or
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

49. (superseded by hand-19 worktree model) **Persistent unclean recovery (hand-11).** `workspace_unclean` and its explicit
    `recovery_action` (`fail|retry`) are folded into `NodeProjection`; an in-scope marker
    makes `resume_action` return `recover`, never `done`. Resume validates BOTH durable
    lease and intent records before mutation, retries intent reversal plus the checked
    quarantine/reset/sweep, and appends an explicit clean result only after every
    postcondition succeeds. `fail` then closes the already-failed attempt; `retry` remains
    a durable non-terminal rerun-pending state until a new dispatch produces its own
    result, including across a crash between cleanup and redispatch. A legacy unclean
    marker with no disposition fails closed for manual repair rather than guessing.

50. (superseded by hand-19 worktree model) **One canonical inplace target model (hand-11).** Every edit is fully resolved at
    plan time and converted back to one workdir-relative canonical target. Intent capture,
    write, rollback/unstage, staging, commit, result files, and receipt paths all use that
    target. Two declarations resolving to the same target are rejected before the first
    write. Internal symlinks therefore edit and receipt their resolved tracked target;
    escapes/gitdir aliases remain rejected. Multiply-linked regular files are rejected:
    inode aliases (including outside-workdir hard links) have no sound single pathname to
    stage/receipt. Modern intents preserve original bytes as base64 and the exact UTF-8
    bytes the attempt expected to write.

51. (superseded by hand-19 worktree model) **Append-only quarantine histories (hand-11).** A quarantine ref is immutable per
    observed tip: its name includes the full tip SHA and creation uses compare-and-create.
    Dead attempts, operator tips observed on redo, and failed rigs that authored commits
    are all preserved before reset; a redo allocates a second ref rather than moving the
    first. Run/node ref components use only `[A-Za-z0-9._-]`; lossy, empty, `.lock`,
    dot-edge, and overlong forms receive a SHA-256 suffix. Thus arbitrary filesystem-valid
    run-directory names cannot make Git recovery fail solely through ref syntax.

52. (superseded by hand-19 worktree model) **Checked filesystem cleanup (hand-11).** Cleanup has no `ignore_errors`: status/diff
    enumeration, directory traversal, byte reads, capture publication, unlink/rmtree, and
    Git reset/ref operations are checked. Every removal has an `lexists` absence
    postcondition. The lease additionally snapshots pre-existing directories, allowing
    attempt-created empty directory roots to be captured/removed and pre-existing empty
    directories to be recreated if an attempt deletes them. Any incomplete Git
    status warning or filesystem failure raises `WorkspaceFault` and enters decision 49's
    persistent halt.

53. (superseded by hand-19 worktree model) **Byte-recoverable immutable capture (hand-11).** Before any destructive reset or
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

54. (superseded by hand-19 worktree model) **Divergent intent reversal (hand-11).** Rollback first compares each canonical live
    target with BOTH the expected attempt bytes and the recorded pre-state. A third state
    (including binary post-crash operator bytes) is durably captured through decision 53
    under `intent-reversal/` before restoration. Parent directories absent at intent time
    are also recorded and removed on rollback; unexpected contents are captured first. A
    changed canonical path topology (for example, an operator-installed parent symlink)
    halts without overwriting it.

55. (superseded by hand-19 worktree model) **Atomic durable-record lifecycle (hand-11).** Lease/intent and capture-manifest
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

56. (superseded by hand-19 worktree model) **Pass-7 crash proof (hand-11).** The transaction regressions use real `fork` plus
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

57. (superseded by hand-19 worktree model) **Active completion certificate (hand-11).** A successful rig's `post_head` must
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

58. (superseded by hand-19 worktree model) **Recovery topology containment (hand-11).** Baseline capture/restore validates every
    intermediate parent against its canonical workdir location before reading or deleting.
    It preflights every manifest path and verifies every raw blob before the first deletion.
    A post-crash parent-symlink substitution or corrupt late blob therefore halts without
    losing live pre-existing bytes or following a path into an external directory.

### Hand-12 calls (from the pass-8 correctness review) — ONE RECOVERY TRANSACTION

59. (superseded by hand-19 worktree model) **One lease-recovery transaction (hand-12, structural spine).** The duplicated live-
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

60. (superseded by hand-19 worktree model) **End-state proof, not command success (B1).** Recovery verifies exact `HEAD == pre_head`
    (or both unborn), byte-mode clean tracked/index status, an empty lease-scoped attempt
    leak set (including created empty directories), and byte-equivalence of every modern
    pre-existing baseline before it can settle or clear. A successful `reset --hard` whose
    non-invertible smudge filter leaves tracked dirt therefore becomes a persistent
    `workspace_unclean` halt. Legacy no-record recovery remains conservative/no-sweep and
    cannot claim a baseline it never recorded; this exception is available only when the
    dispatch explicitly lacks the modern required-lease marker.

61. (superseded by hand-19 worktree model) **Reversal alias and portable case policy (B2/H5).** Intent validation re-stats every
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

62. (superseded by hand-19 worktree model) **Checked, byte-safe, object-format-aware Git reads (B3/H1/H4).** Every `rev-list` and
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

63. (superseded by hand-19 worktree model) **Required modern records and settle-before-clear (H2/H3).** New `Dispatched` events
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

64. (superseded by hand-19 worktree model) **Uniform capture integrity (H6).** Baseline, failed-diff, quarantine, and intent-
    reversal manifests all carry a canonical outer SHA-256 digest. One checked loader
    validates manifest identity/path topology plus every file blob's size and SHA-256;
    baseline restoration and public forensic round trips share it. Atomic publication
    remains the torn-write defense; the digest/blob checks add post-publication corruption
    detection. No pass-8 intended direction was otherwise changed.

### Hand-13 calls (from the pass-9 correctness review) — CLOSE THE TWO TEARS

65. (superseded by hand-19 worktree model) **Per-path sweep proof (hand-13).** Every absent-prestate inplace target receives a
    signed `IntentWrite.swept=True` update, fsynced immediately before the checked leak
    unlink that covers that path. Restart accepts a disappeared started target only when
    this per-path proof (or the legacy aggregate completed-sweep proof) is durable; an
    unmarked disappearance remains the hidden-hard-link ambiguity and fails closed. Sweep
    preparation revalidates target topology/link counts before publishing each proof, and
    directory leak roots mark every intent target they contain. The aggregate `swept`
    marker remains the all-path completion boundary, not the only redo classifier.

66. (superseded by hand-19 worktree model) **Required-record settlement before torn integration (hand-13).** A torn OK
    `result` may reconstruct `pre_head..post_head`, but before publishing its recovered
    `Integrated` it must validate the dispatch's required lease/intent records (or an
    immutable settled/recovery replacement). It then publishes an integrity-bound,
    create-once completion-settlement certificate containing those validated records and
    settles the active records *before* `Integrated`. A crash during settlement or after
    settlement but before integration reuses that certificate and safely redoes both;
    missing marked records with no replacement raise `WorkspaceFault` and leave the epoch
    open. This extends hand-12 H3's settle-before-publication rule to the success tear.

### Hand-14 calls (from the pass-10 correctness review) — CLOSE THE EFFECT CHANNEL

67. (superseded by hand-19 worktree model) **Verified read-only predicates (hand-14).** Every command `until` runs under a
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

### Hand-15 calls (from the pass-11 correctness review) — VERIFY AND REAP

69. (superseded by hand-19 worktree model) **Index-independent predicate verification (hand-15).** Lease open byte-snapshots the
    real repository index and hashes actual tracked worktree content through a fresh
    temporary index seeded from `HEAD`; pre/post write-tree OIDs are compared without
    consulting live-index hints, and a raw-byte digest prevents clean filters from hiding
    byte changes. Verification always restores the exact index bytes.
    Recovery captures paths from the temporary-tree delta, resets/restores content, restores
    the exact index snapshot last, and verifies both proofs. `assume-unchanged` and
    `skip-worktree` therefore cannot hide a predicate mutation or survive its recovery.
70. (superseded by hand-19 worktree model) **Predicate process barrier (hand-15; SUPERSEDED by hand-16 entry 73's general
    barrier).** Every command predicate has a positive timeout
    and runs in a new session behind a session-leader supervisor. The effect command remains
    gated until `(pid, pgid, /proc start-time)` is atomically written and fsynced under
    `run_dir`. Normal completion and timeout SIGKILL the whole group before verification and
    settle the record. Every lease recovery first loads the record, proves PID start-time
    identity (never killing a recycled PID), SIGKILLs the group, and waits for termination.
    The reaper is kill-only: it never captures, resets, restores, or otherwise mutates Git or
    worktree state; those remain exclusively `recover_lease` phases.
71. **Durable torn-tail repair (hand-15).** Any unterminated final journal record is uncertain
    and discarded, even when the available bytes happen to parse as valid JSON. Before a
    fresh append owner returns, `Journal.load` opens the file read/write, truncates to the
    last newline-terminated record, and fsyncs the file plus its containing directory.
    Malformed complete/middle records
    still raise; append can no longer concatenate onto accepted torn bytes.
72. **Create/load ownership split (hand-15).** `Journal(run_dir)` is creation-only and raises
    typed `JournalExistsError` when `events.ndjson` is already nonempty. `Journal.load` is
    the sole continuation path: it classifies/repairs the physical tail, replays projection
    state, and returns the fresh append owner.

### Hand-16 calls (from the pass-12 correctness review) — ONE PROCESS BARRIER

73. (superseded by hand-19 worktree model) **Unified process supervision (hand-16).** The predicate-only launcher/reaper is
    deleted. Every built-in external workload command — rig command or predicate — uses
    one workspace-owned `ProcessSupervisor` seam while `Rig.run(prompt, workdir)` remains
    unchanged. A session-leader guardian first reports ready, then the parent atomically
    publishes and fsyncs a signed `processes/` identity record containing
    `(epoch, node, attempt, pid, pgid, /proc start-time)`; only then does the parent open
    the effect gate. The guardian delivers one bounded framed result and remains alive
    until the parent kills the group. A broken/partial result-pipe write makes the guardian
    SIGKILL its own group rather than abandon descendants. Shell/Script rigs never create
    an escaping group; their direct-child timeout is enclosed by the same outer barrier.
    Deliberate `setsid` escape remains outside the declared process-group model.

    Normal completion reaps before workspace verification/integration. Every
    `recover_lease` starts with the same kill-only reaper, before capture, Git/disk
    recovery, lease settlement, or redispatch. The reaper never touches Git or workspace
    bytes. For every recorded state, including a vanished leader with surviving members,
    it repeatedly enumerates `/proc`, selects non-zombie processes whose PGID equals the
    record and whose start-time is at least the recorded launch start-time, rechecks and
    SIGKILLs each through a pidfd (closing the stat-to-signal reuse race), and verifies that
    no eligible member remains before fsync-settling the record. An extant recorded PID
    with a different start-time proves numeric-generation recycling and is never signalled.
    Leader `getpgid` ESRCH is an absent/recheck race, not a raw restart failure. This
    supersedes decision 70's predicate-specific mechanism and makes the M2
    `ProcessSupervisor` authority explicit while M1 execution remains serial.

74. **Fresh-load durability adoption (hand-16).** `Journal.load` does not infer that a
    newline-terminated tail was previously durable: it may be the complete page-cache
    residue of a poisoned append whose file fsync or first-file directory fsync failed.
    After parsing and any torn-tail truncation, a fresh owner unconditionally fsyncs every
    accepted existing `events.ndjson` (including an empty file) and then its directory
    before returning. A sync failure returns no owner and no accepted durability fact.

### Hand-17 call (from the pass-13 correctness review) — COMPLETE PROCESS BARRIER

75. (superseded by hand-19 worktree model) **Core Git joins the complete barrier (hand-17).** Every engine-owned external launch
    site is now either a built-in rig/predicate operation under the hand-16 workload
    supervisor or a core Git/index command under a recorded core scope; the Git helpers
    fail closed outside that scope. One core record is shared by the whole active
    `run_epoch` invocation rather than written per Git command. Its private session-leader
    guardian accepts a fixed framed Git protocol, and after each command it pidfd-kills and
    verifies the absence of every same-PGID descendant before replying, so a hook/filter
    cannot mutate between Git completion and integration. The scope is reaped and settled
    before `boundary(closed)`.

    Recovery uses that invocation's live core scope as its recovery scope. After fresh
    journal adoption, engine construction kill-only reaps every prior-owner process record
    before any Git/byte recovery is reachable; `recover_lease` repeats the prior-attempt
    workload reap plus the prior-owner sweep before loading recovery inputs. `ProcessRecord`
    now distinguishes `workload|core`, carries an opaque core `scope_id`, and records the
    owner's PID plus `/proc` start-time. The prior-owner sweep excludes only records whose
    complete owner generation matches the current engine, so it cannot reap/deadlock on its
    own live recovery scope. Reaping remains process-only and precedes capture, reset,
    restore, settlement, redispatch, and every other workspace mutation.


### Hand-19 calls — per-node worktree model

76. **Disposable attempt worktrees.** Every `do`, `inplace`, and command predicate starts
    from the exact run-branch tip in a fresh uniquely named detached worktree under
    `run_dir/worktrees/`. No path is reused. Failure and interruption abandon that tree;
    best-effort `git worktree remove --force` is garbage collection, never recovery.
77. **Linear fast-forward integration.** Successful node commits must be one linear
    descendant range. The core journals `result`, advances the run branch by checked-out
    `merge --ff-only` or un-checked-out `update-ref` CAS, then journals every commit/path in
    `integrated`. Cherry-pick is excluded because rewritten SHAs and conflict state would
    recreate recovery machinery. Actual dispatch remains serial.
78. **Exact-tip resume verification.** Opened boundaries pin run ref/base. Resume verifies
    commit existence, exact ranges, paths, and newest tip with plain subprocess Git. A
    result/fast-forward tear is reconciled only at exact base or candidate. Unverifiable
    inactive claims append one tail-invalidating fallback boundary and rerun; an
    unknown/live unverified tip raises a typed error. Operator activity is never reset or
    quarantined.
79. **Small process and path boundary.** Built-in external commands use only an in-process
    process group with timeout kill; no durable process record, guardian, reaper, or core
    Git scope remains. Escaped orphans can write only abandoned worktrees. Receipt paths
    are ordinary UTF-8 strings; the arbitrary-byte wire codec and transaction records are
    gone. `inplace` still validates lexical/runtime containment and declared commit paths.
80. **Deliberately dropped scenario families.** The rewritten suite removes shared-workdir
    quarantine refs/reset histories; lease/baseline/capture manifests and failed-diff
    forensics; durable inplace intents/reversal, index snapshots, hard-link/case/topology
    recovery; `workspace_unclean` halt/settlement states; smudge-filter and hook-in-core-
    commit recovery; durable process records/guardian/reaper/core scopes; predicate
    index/read-only mutation proofs; marker scans; and arbitrary-byte path encoding. These
    test implementation machinery that no longer exists, not current behavior.
81. **Retained scenario corpus.** Admission/dealias/upstream/totality rules, torn-tail
    repair and append poisoning, result→integrated crash windows, receipt verification,
    loop caps/floors/nesting, retry in a new path, orphan-write harmlessness, exact
    run-branch ownership, Script/Shell busy+timeout contracts, and result artifacts remain
    executable regressions.

### Hand-20 calls — prefix rewind and integration lifetime

82. **Known-prefix fallback, parent-bound ref movers, and linked-worktree refusal.** Resume
    retains every verified integration tip and the first dispatch that depends on it. A
    live ref at one of those exact earlier tips appends the existing tail-invalidating
    fallback boundary and reruns only that suffix. A ref still naming the journal's current
    claim after that commit object disappears is read as a raw claim, restored by a
    compare-and-swap to the preceding verified tip (also restoring the checked-out tree),
    and takes the same fallback. An unknown descendant, side commit, or unknown missing
    claim remains a typed `BranchDivergedError`; operator history is never reset.

    On Linux, only Git subprocesses that can move the run ref (`merge --ff-only` and
    `update-ref`) arm `PR_SET_PDEATHSIG=SIGKILL` and immediately verify their post-fork
    parent PID before exec. Thus a killed engine cannot leave its integration command to
    land after a replacement engine durably invalidates that attempt. A CAS-only change
    cannot fence the checked-out merge and does not fence an old CAS when fallback leaves
    the ref at the same base; parent-bound lifetime is the smaller complete mechanism. No
    process records, reaper, guardian, intent, lease, or supervision scope returns.

    Before integration selects either backend, `git worktree list --porcelain -z` finds
    every linked-worktree owner. The supplied worktree may receive the checked-out merge;
    if another worktree owns the run branch, integration refuses with the existing typed
    integration/node failure and leaves both the ref and owner worktree untouched. An
    unowned named branch continues to use compare-and-swap `update-ref`.

### Hand-21 calls — ownerless Git-lock recovery

83. **Resume-proven, live-owner-safe Git-lock cleanup.** Checked-out integration keeps
    `merge --ff-only`; changing the supplied worktree into an anchor would break the M1
    contract that its checked-out run branch and files advance together. Only while
    resolving one of two journal-proven interrupted mutation windows — an effectful result
    at the exact base whose integration did not land, or a missing current claim being
    restored to its verified predecessor — may the engine remove the supplied worktree's
    `index.lock`, the run ref's `.lock`, or merge's `ORIG_HEAD.lock`. An `index.lock`
    additionally requires the run branch still be owned by the supplied worktree. Every
    lock must belong to the engine's uid, `/proc` must conclusively show no live same-uid
    Git process, and its pathname must still name the observed inode immediately before
    unlink. Otherwise recovery preserves the lock and raises typed
    `RepositoryTransientError`. Each lock directory is fsynced after unlink (and when
    adopting an already-absent lock) before fallback can be journalled, so durable fallback
    never outruns durable cleanup. A new ordinary Git writer cannot acquire an
    already-existing lock; an existing live operator is caught by its Git process even in
    Git's close-before-rename interval. A filter descended from the already-dead Git process
    may retain an inherited descriptor, but it does not own Git's lock protocol and cannot
    land the ref move. This rule excludes an operator manually deleting/replacing Git's lock
    file concurrently with resume; manual lock surgery must occur only while the engine is
    stopped. Successful cleanup is named in the fallback boundary. Before journalling a
    checked-out result→integration fallback, every receipted path's index and worktree state
    must match either the verified base or the interrupted candidate; a third state is
    preserved with a typed transient refusal. Receipts disable rename collapsing so both
    sides are restored; ignored additions and candidate file mode/type/blob (including
    symlinks) are verified before removal, symlinked parent paths are refused, and
    file↔directory transitions remove only verified candidate leaves/empty collision
    directories. A missing path with the index already at base is an idempotent
    restoration-in-progress state, so a second constructor can finish after another kill.
    Parent-bound, path-scoped Git reset/restore then
    returns those paths to the base and removes only verified candidate additions, so a
    mid-checkout kill cannot leave unjournalled candidate files behind and unrelated
    worktree state is untouched. The restored index, files, deletions, and containing
    directories are fsynced before the fallback boundary.

84. **Missing-claim restore has the same lifetime and residue boundary.** Its checked-out
    `read-tree --reset -u` now uses the parent-bound launcher already used by ref movers.
    A killed constructor therefore cannot leave a live orphan index writer. Resume applies
    rule 83 to any dead child's residue before retrying; a live/uncertain owner, failed
    index update, or unchanged claim after failed CAS is a typed transient retry rather
    than a raw Git `RepositoryError`. Pre/post restore index paths are unioned when fsyncing,
    so removed candidate additions' parent directories are covered. Restored tracked state
    and the CAS-updated ref are fsynced before fallback publication; every fallback also
    re-syncs the current run ref,
    covering a crash after CAS but before its first sync. A different live claim remains
    typed divergence.

85. **M2 residue rule.** Parent death fences engine-owned ref-moving/index-writing Git
    children but is not filesystem cleanup. Checked-out integration now has an explicit
    crash-residue story: exact journal evidence authorizes only ownerless-lock removal;
    every live or uncertain lock is preserved for retry. Disposable attempt-worktree locks
    remain harmless because paths are never reused. This adds no lease, intent, quarantine,
    process record, or new integration transaction. It does not serialize an operator who
    starts new Git work after the liveness scan, nor close hand-20's worktree-ownership
    scan-to-ref-move TOCTOU; M1's operator model remains activity between runs, and M2 still
    needs explicit coordination before concurrent integration or operator-facing worktrees.

### Hand-22 calls — bounded sibling concurrency

86. **One coordinator, bounded workers.** `Engine.max_workers` defaults to one, preserving
    the M1 traversal exactly. At values above one, a `Dispatch` leaf group is opened at one
    verified run tip: the coordinator creates and journals every fresh detached attempt at
    that same tip, worker threads run rigs and build candidate receipts only, and the
    coordinator consumes completions. No worker appends the journal or moves the run ref.
    Results stay keyed by input position (`ExecutionOutcome.children`); a loop always emits
    the explicit `body_result_seq`, so completion order cannot select its body artifact.
    Recursive `seq` and `loop` traversal still gates downstream groups on integrated
    upstream work; nested Dispatch groups parallelize whenever that leaf frontier is
    reached. Composite siblings not themselves representing one worktree continue through
    the same serial recursive traversal rather than introducing a second execution engine.

87. **Disjoint reapply combine.** Siblings in one group own the exact union of paths in
    their source receipts. The first completion with a successful effect lands normally.
    Before every later landing, the coordinator re-verifies its source receipt and rejects
    any intersection with paths already landed by that group. It then cherry-picks the
    source's linear commits, one by one with empty commits retained, into a fresh detached
    throwaway worktree at the moving tip. The throwaway is discarded on conflict; no
    sequencer recovery exists. The resulting commit count/per-commit paths must equal the
    source receipt, and only those rewritten SHAs are fast-forwarded and journalled. The
    result records `integration_base`, additive and legacy-defaulting to `pre_head`, so
    replay verifies the actual landing range rather than the shared source range.

88. **Interleaved resume and ownership boundary.** The append stream remains serialized.
    A crash can therefore leave at most one successful result awaiting its ref move; its
    fallback starts at that result when it used a moving integration base, retaining prior
    sibling integrations and dispatch facts. Dispatched-only siblings based on an earlier
    verified group tip receive a failed interruption result on resume instead of a global
    tail rewind, then rerun individually from the current verified tip. An advisory
    common-Git-dir `flock`, keyed by run ref and held across constructor reconciliation or
    an epoch execution, serializes cooperating Wildflows owners. After waiting for that
    lock, an older Engine reloads any newly accepted journal stream before it can append.
    Raw Git mutation during an active run remains outside the operator-between-runs contract
    and CAS/ownership checks still fail closed where observable.

89. **Checked-out merge trim deferred.** The owner-endorsed CAS-only simplification was
    considered and not taken in M2. Refusing a checked-out run branch would change the M1
    public/default path and force broad fixture migration; a final root sync would recreate
    the index/filter crash boundary it aimed to delete. The already-tested hand-21 restore
    classifier remains live because checked-out integration remains supported. This hand
    spends its line budget only on sibling execution/combine/resume and the small advisory
    coordination boundary; CAS-only integration remains a separable future raze.

### Hand-23 calls — planner integration and run loop

90. **Planner output durability and admission.** A strict `PlannerDecision` carries an
    expression (or an ending summary), typed deadline/max-epoch rails, rationale, and end
    flag. The rig's `Result.text` is atomically retained before parsing (exact stdout on a
    successful ScriptRig call; its selected error surface on nonzero). Expression parsing
    plus the existing admission pass remain the hard no-effect boundary; rejection is a
    retryable typed `PlannerFailure`. Opened boundaries add validated rails/rationale.

91. **Open epoch outranks planner invocation.** `Run` first replays the journal. If an
    epoch is open, it executes that boundary's exact expression and never invokes the
    planner for it. Only a closed epoch permits the next prompt. This is the crash rule for
    planner-output-through-mid-epoch death and keeps the epoch boundary both re-entry and
    durability point.

92. **Ask and setup use the existing vocabulary.** Ask appends `asked`, leaves the epoch
    open, and raises `AwaitingOwner`; `answered` projects directly into the Ask node's
    result with no duplicate `result` event. Setup appends host-marked `dispatched` plus a
    bounded-tail `result`, runs through the existing supervised shell path at repository
    root, and never uses a node worktree. A failed idempotent setup is an ordinary
    retryable node; an interrupted/failed non-idempotent setup raises
    `SetupResumeRequired` until the owner explicitly approves its retry.

93. **Bounded planner digest and artifact reference.** The next prompt contains latest
    effective per-node results from only the immediately preceding epoch. Text, paths, and
    node count are deterministically capped with explicit original counts/truncation
    markers. Ordinary result events add a run-relative path to their already-fsynced full
    JSON artifact; fallback-invalidated raw history is excluded.

94. **Macros remain data.** Builtin and run-adjacent JSON macros expose only name,
    description, and source path to the prompt. Placeholder substitution/expansion is not
    core behavior: the planner reads a fitting template and emits normal expression JSON.

### Hand-24 calls — executable combine, journal v1, and real adapters

95. **Combine is result-fed Do.** The engine executes `inputs`, flattens their durable
    result keys in declaration order, requires each result to be successful, then builds
    an internal `Do` at the Combine node id. Its prompt appends full result text, artifact
    metadata, the run-relative artifact link, and its contained absolute path. The ordinary
    dispatched/result/integrated transaction and projection own completion and resume;
    there is no Combine event or state machine. A failed dependency raises typed
    `CombineDependencyError` before the combiner starts.
96. **Journal vocabulary v1.** Every record carries integer `version: 1`; load refuses
    missing or mismatched versions with `IncompatibleJournalError`. No migration path is
    inferred and the existing event kinds are the frozen v1 vocabulary.
97. **Adapters remain scripts.** `rigs/` contains self-contained picodex planner/senior
    and local OpenAI-compatible worker adapters. Prompt bodies cross into `pi`/`curl`
    through stdin, process-group handles and Git ceilings match the ScriptRig boundary,
    and nonzero quota signatures retain the existing `busy` classification. Model calls
    are operator smoke tests; repository tests use fake transports on `PATH`.

---

## 12. v2 — the frame architecture (call-stack pivot, 2026-07-14)

Owner-driven redesign settled live during DF1. Supersedes the epoch/expression
execution model (§1–§4) as the target architecture; v1 remains the shipped
baseline until v2 lands. The trigger: DF1 ran `loop(senior)` and the engine
decapitated a working senior between iterations — grindstone's cold-attempt
model wearing a new name. The founding requirement was an always-alive,
in-context senior; disk-journal "resume" of a mind is not resume (owner:
"you can't construct resume using disk journalling, it simply does not work").

### The model

98. **A run is a call stack of frames, not a declared expression.** A frame is
    a resident agent process (pi/claude -p/codex exec) working in its own
    worktree on its own branch. When a frame needs subordinate work it calls an
    engine tool; the blocking tool call IS the bank — the parent's context
    stays live in RAM at zero token cost while children grind. Structure is
    discovered (journaled as it happens), not forecast. The planner dissolves
    into the root frame: the war general runs the campaign in one persistent
    head.

99. **Most v1 primitives dissolve into agent control flow.** `seq` = the
    caller makes calls in order; `loop` = the caller's own while(); `combine` =
    the parent reading child results in-context. The engine tool surface is
    exactly three (+setup as a dispatch flag): **`dispatch(tasks[], rig,
    parallel?)`** — push child frame(s), block, return result text;
    **`gate(cmd)`** — run a deterministic check in the caller's worktree,
    return exit + full stdout AND stderr; **`ask(question)`** — park the frame
    for the owner. Loops-as-engine-control-flow (`loop(senior)`) are the named
    anti-pattern; engine-visible retry loops over disposable one-shot juniors
    remain legitimate *inside* a parent's while().

100. **Worktrees stack with the frames.** A child's worktree branches from the
    parent's frame branch (never run root); on unwind the child's commits
    integrate into the parent's branch only. Parallel siblings within a frame
    keep the v1 disjoint-ownership merge. The run branch advances only when
    the root frame commits. Worktrees live OUTSIDE the target repo tree
    (DF1 defect: nested worktrees let Node resolution silently borrow the host
    checkout's node_modules; the until-gate false-red that cost DF1 a wasted
    second senior).

101. **"State machine disposes" moves to the call boundary.** Every dispatch
    passes engine admission: depth cap, breadth cap, per-subtree spend/time
    rails, rig allowlist. The engine can refuse a frame. Every push/pop is a
    journal event; child work is durable per-frame. Gates journal exit code
    plus BOTH output streams (DF1 defect: stdout-only capture made the matrix
    script's fatal error invisible).

102. **Resume = replay with memoized tool calls.** No session-file
    resurrection. On crash, the engine restarts each ancestor frame with its
    original prompt + a structured digest of its completed calls and results
    ("resume event — do not re-issue; continue"), carried by the default frame
    skill so every frame handles resume identically (ONE ENTRY PATH scar:
    resume classification lives in the engine, never per-case prompts). If a
    resumed frame re-issues a completed call anyway, the engine matches it
    (frame + call index + content hash) and returns the journaled result
    instantly — the engine refuses to pay twice. Only the in-flight frontier
    frame's uncommitted work is ever lost. Resident processes are thereby a
    performance tier, not a durability requirement.

103. **Standalone engine; MCP is the frame boundary.** The orchestrator is NOT
    a pi extension: the supervisor must outlive the supervised (the engine
    replays the stack; a passenger cannot). The engine serves one per-run MCP
    endpoint (localhost + run token); the frame contract is "any agent process
    that reads a prompt, works in a CWD, can call MCP tools, exits with text."
    pi is the reference frame implementation (optional thin adapter shim, e.g.
    abort-signal kill semantics); claude -p and codex exec are peers, so
    heterogeneous stacks (claude root for vision/judgment, Sol sub-seniors,
    local qwen juniors) are first-class. Rig-level: frames get per-frame
    compaction/context settings via their CWD without writing .pi/ into gated
    diffs; senior frames pin their pi-subagent roles to local backends (with
    flock GPU pinning) for free in-context micro-delegation — that agentic
    layer stays invisible to and ungated by the engine, by design.

### Evidence

104. **Mechanism smokes (2026-07-14).** Depth-1: headless `pi --print` with a
    20-line extension banked 10s on a blocking custom tool and resumed with
    the result verbatim. Depth-2: root pi → dispatch tool → engine spawned a
    real child pi in a stacked worktree, child committed, engine ff-merged
    into the parent branch on unwind, parent woke and verified the commit in
    its own CWD. Both first-shot.

105. **DF1 (dogfood 1) — WON, and the autopsy priced the pivot.** The senior
    fixed the arc-stage defect family grindstone left acceptance-RED after 8
    epochs/~5h: one iteration, 20m40s launch-to-integrated (`edaa7526`),
    independently verified in a fresh worktree (Lesson 255/255, hang guard,
    viewport matrix 4/4, rc=0). Both failures in the run were engine, not
    model: the stdout-only false-red gate (§101) and the cold re-dispatch of
    a fresh senior with an identical prompt (§98 exists to delete this).

### Hand-29 calls

106. **V2 is one hard vocabulary cut.** `events.ndjson` now accepts only
     `version: 2` records. The vocabulary is run-open/finish, frame
     push/exit/integrating/integrated/pop, and typed dispatch/gate/ask
     call/return facts. The fsynced append owner, torn-tail repair, contiguous
     sequence rule, and one live/replayed projection remain; all v1
     epoch/node/expression readers are deleted and a v1 stream raises
     `IncompatibleJournalError`.

107. **Logical call identity is transport metadata, not model convention.** The
     generated Pi shim supplies a hidden monotonic call index in MCP `_meta`;
     the engine computes the canonical content hash after typed validation.
     Completed calls join on `(frame_id, call_index, content_hash)`. On replay
     the shim receives prior exact calls so an accidental exact reissue can
     reclaim its old index, while the ordinary next index starts after the
     durable prefix. A reused index with different content fails typed at the
     call boundary and never executes.

108. **Root gets the same stacked-branch contract as every child.** The run ref
     is the owner-selected existing branch; `f0` works on
     `wildflows/<run-id>/f0`, and every deterministic child branch is cut from
     its caller's current frame tip. Child integration advances only the
     caller branch. The run ref advances only from the root unwind. Branches
     remain as durability evidence; attempt worktrees are unique, disposable,
     and rooted under a repository-hashed system-temp directory (or an
     explicit external root), with containment rejection/fallback if that
     path would be inside the target repository.

109. **Frame integration has a durable pre-move intent.** A verified source
     receipt is converted to the exact landed candidate, then
     `frame_integrating` records `(target, base, candidate, source, landed)`
     before the ref move. Resume accepts only target-at-base or
     target-at-candidate and then appends `frame_integrated`; a third tip is
     divergence. A pending integration intent already reserves its source path
     ownership during parallel replay. Serial children fast-forward. Parallel
     siblings start at one caller tip, intersect exact source path ownership, and cherry-pick a
     disjoint later source through a throwaway external integrator worktree;
     only landed SHAs enter the parent history.

110. **Admission uses declared per-rig reservation units.** The call boundary
     enforces depth, immediate breadth, descendant-frame count, inherited
     subtree deadline, registry allowlist, and cumulative subtree spend. Until
     rigs report trusted token/cost telemetry, one child reserves one unit by
     default and `AdmissionPolicy.rig_costs` may assign a different positive
     declared unit. Refusals are memoizable `dispatch_returned(outcome=
     "refused", error_code=...)` tool results and allocate no frame/worktree.

111. **A clean caller is the durable call floor.** Dispatch, gate, and ask
     require the caller frame branch/worktree to be clean before the call, so
     ancestor state survives replay as commits. `gate` runs in that exact
     worktree, returns/journals full stdout and stderr independently, and a
     nonzero exit remains data rather than an MCP transport error. An
     interrupted gate is deterministically rerun; a completed gate is
     memoized. `ask` appends the question, blocks the resident request, and
     observes an atomically published answer file; the ask handler appends the
     answer before returning it, so the same file also feeds a replayed ask.

112. **Pi is a generated per-attempt adapter, not the supervisor.** The engine
     binds one authenticated `127.0.0.1` ephemeral JSON-RPC/MCP endpoint and
     writes a mode-0600 TypeScript extension in run runtime state with URL,
     token, frame id, and replay index data baked in. `worker-picodex.sh` loads
     it with `pi -e`; `worker-local.sh` remains a no-tool one-shot leaf. No
     extension, `.pi/`, or engine runtime file is created in a frame worktree.

113. **The v1 raze includes owner surfaces.** Planner decisions, expression
     models/admission/traversal, macros, planner adapter/docs, and epoch CLI
     flags are removed. `run` starts the root rig directly from `job.md`, and
     `resume` replays that root stack. The dashboard backend is deliberately a
     compiling v2 journal/status stub; a frame-stack UI is the next phase, as
     allowed by §10/§12.

114. **The run capability is attenuated per active frame.** The endpoint owns a
     random run secret, but each frame attempt receives a separate random
     bearer registered to exactly its engine-owned frame id. The claimed frame
     header is checked against that binding, so a child cannot spend through a
     blocked parent's worktree. Capabilities are revoked on frame exit. This is
     attenuation of the required per-run authenticated endpoint, not a second
     endpoint or agent-owned identity channel.

115. **Memoization and subtree admission are single-flight reservations.** A
     condition-guarded leader owns each `(frame, call_index)` from identity
     check through durable return; exact followers wait and receive that one
     result, conflicting content is a typed protocol error, and later indexes
     cannot pass a pending earlier call. Dispatch admission atomically reserves
     frame/spend units against the caller AND every ancestor subtree. Each
     `frame_pushed` consumes one reservation into the durable descendant fold;
     unlaunched residue is released. Resume reconstructs the still-unlaunched
     reservation of every durable pending dispatch before a child can call.
     Parallel source-path ownership is derived
     from durable sibling integrations on replay and updated under the same
     serialized integration lock as overlap checking.

116. **Answers and process death are first-writer/fail-closed boundaries.** An
     owner answer is fsynced to a private temp and published with no-replace
     link semantics; a second answer is refused, while a crash after publication
     leaves the same complete answer for replay. A live CLI answer uses a
     read-only complete-record snapshot and never invokes journal torn-tail
     repair beside the resident append owner. External workloads use a
     parent-death-bound leader plus same-process-group watchdog, so supervisor
     death kills ordinary descendants while abandoned worktree paths remain
     never reused.

### Hand-30 calls

117. **Skills are prompt data, not capability.** A dispatch accepts an optional
     ordered skill bundle for EACH task (`skills: list[list[str]]`); omission is
     canonically one empty bundle per task. Per-task bundles are required because
     siblings at different tiers need different steering. Skill names do not
     participate in admission, rig allowlists, or spend reservation. The library
     is frontmatter-free Markdown loaded from `<target>/.wildflows/skills/*.md`
     over the packaged `wildflows/skills/*.md`; a repository file shadows a
     bundled file with the same stem. The filename stem is the name and the
     required first `# title — one-line description` heading supplies the
     manifest description.

118. **The engine owns one prompt assembly order.** Every launch, including a
     replayed frame, receives: assigned skill files in declared list order and
     in full; then the frame job/task; then a deterministic `SKILL MANIFEST` of
     every resolved library name and first-line description; then the engine tool
     preamble. This final preamble carries replay instructions/digest. A committed
     `progress.md` is included in that digest on replay. The manifest is the
     discovery surface by which a frame can select small bundles for children;
     stock `skill-selection`, `long`, and `plan-compress-execute` skills ship as
     package data.

119. **Skill identity is durable call identity.** `frame_pushed` records the
     frame's assigned skill names. Validated dispatch requests normalize empty
     bundles before the canonical SHA-256 is computed, so omitted and explicit
     empty bundles share one identity while the same tasks with different skill
     names or order are a cache miss. Projection replay retains the bundles and
     every dispatch entry in a resumed frame's call digest shows the per-task
     skills it used. A resumed child must match its durable prompt, rig, task
     position, and assigned skills.

120. **Banked MCP calls use chunked whitespace heartbeats.** A valid id-bearing
     `tools/call` starts an HTTP/1.1 chunked JSON response before execution, runs
     the typed handler independently of the client socket, writes a whitespace
     chunk every 15 seconds while pending, then writes one JSON-RPC object and the
     terminal chunk. JSON permits the leading whitespace, and periodic body bytes
     survive intermediary/node-fetch idle timeouts without a second protocol.
     Broken-pipe/reset writes end only that response stream: the engine operation
     continues to its durable memoized return and produces no disconnect traceback.

121. **The Pi shim reconnects one frozen request.** It allocates the hidden call
     index once, serializes one request body, and retries that exact body with
     bounded exponential delay after fetch rejection, HTTP failure, interrupted
     or malformed JSON, or malformed response/result framing. None of those
     transport failures becomes model-visible tool output. A valid JSON-RPC
     engine/protocol error remains terminal; ordinary admission refusals and rig
     failures remain typed tool results. An explicit Pi abort stops retry rather
     than spinning and seals that attempt's local call frontier, so no later index
     can pass an ambiguously delivered call; process replay reconstructs the
     durable frontier. A live retry preserves `(frame, index, canonical hash)` and
     joins the engine's existing single flight. Distinct concurrent later indexes
     wait behind an earlier in-flight index even before its called event is durable.

### Hand-31 calls — frame-call-stack operator console

122. **The dashboard is a journal projection, not a second state machine.** It reads
     only complete newline-terminated v2 records, validates contiguous sequence and
     the typed event vocabulary, then folds the existing `RunProjection`. Frame and
     call display states (running leaf, banked caller, parked ask, terminal outcome)
     are derived from that fold. An unterminated tail is ignored in place; the
     dashboard never invokes journal repair or writes watched run state.

123. **Canvas geometry follows discovered calls.** A frame is rendered before its
     calls. Dispatch calls stack vertically by call index; one call's parallel tasks
     occupy one horizontal sibling row. A completed dispatch return is the single
     collapse trigger for the whole sibling row, never an individual child's exit.
     The first five slots plus a counted ghost bound broad rows. Nesting stops after
     three relative levels and a visible drill-in action rebases that frame as the
     canvas root, with ancestor breadcrumbs to climb back.

124. **Run identity is repository-qualified.** One dashboard watches a deduplicated
     ordered set from repeatable `--repo` flags and/or a line-oriented watchlist.
     Public identity is `(repo_id, run_id)`, so equal run ids in different targets
     remain distinct in APIs, the picker, LIVE NOW, SSE, and artifact routes. Port
     8181 is the fixed default. SSE resumes strictly after sequence ids and sends
     idle comments without becoming another event vocabulary.

125. **No new control surface is implied by an operator console.** The only mutation
     route is an owner answer delegated to the existing
     `Run.deliver_live_answer` seam and guarded by a startup token. It targets the
     exact `(frame_id, call_index)` pending ask. The console deliberately provides
     no launch, pause, kill, or retry API.

126. **The approved v1 visual tokens survive the v2 topology.** Both exact light and
     dark token sets are declared and selected by `data-theme`; light ground remains
     cool white `#fbfcfd`. Dot grid is canvas-only, zone color sits on components,
     and panes remain neutral. Cards have no left-edge accent bar. A running leaf
     alone breathes on a slow two-second cycle; reduced-motion replaces animation
     with a strong static violet outline. Banked violet, parked amber, failed red,
     collapsed grey/green, and queued muted states stay distinguishable without
     relying on motion.

127. **Journal language is operator-facing.** Frame ids display as breadcrumb paths;
     a gate return says `gate: PASS/FAIL (exit N)` and expands both captured streams.
     Running durations tick from push time while terminal durations stop at exit.
     Result prose clamps to two lines until expanded, event kinds use the approved
     palette, and dispatch calls/returns carry explicit request/result margin refs.
     The tracked synthetic repository exercises failure, owner parking, a 20-slot
     fan-out, whole-call collapse, a two-stream failed gate, and depth four.
