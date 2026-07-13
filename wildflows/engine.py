"""The minimal engine (ladder step 2) + replay.

Executes an expression tree of do/inplace/seq/dispatch/loop nodes, journalling every
event in the single vocabulary. Effects are core-mediated: after a rig runs, the CORE
(never the model) stages + commits the worktree's changes and emits `integrated` — the
deterministic record is the committed diff, never the rig's claim.

Resume = fold-the-journal. On construction the engine loads any existing journal
(seq continues strictly-increasing across restarts) and folds it into per-(epoch,
node_id) state; `run_epoch` re-enters an already-opened epoch without re-executing its
completed nodes. `replay` reconstructs the same state from the ndjson alone, proving
resume needs no per-shape code.

Executable here: `do`, `inplace`, `seq` (strict order), `dispatch` (parallel semantics,
executed serially in the PoC), and `loop` (with a `cmd` predicate). Combine/ask/setup
and `loop` with a `flag` predicate raise NotImplementedError.
"""
from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from wildflows.events import (
    Boundary,
    Dispatched,
    Event,
    Integrated,
    LoopIter,
    ResultEvent,
)
from wildflows.expr import (
    CtxRef,
    Dispatch,
    Do,
    Expr,
    Inplace,
    Loop,
    Seq,
    Until,
    assign_node_ids,
)
from wildflows.journal import Journal
from wildflows.rig import RigRegistry, Result

# A floor high enough that no real journal seq exceeds it: used to DISABLE resume-skip
# for a fresh loop iteration (every iteration past the resumed-partial one re-runs its
# whole body, regardless of that node_id's stale state from an earlier iteration).
_NO_RESUME = sys.maxsize


@dataclass
class _Integration:
    """The outcome of a core-mediated git integration."""

    status: Literal["committed", "noop", "failed"]
    commit: str | None = None
    paths: list[str] = field(default_factory=list)
    stderr: str = ""


class Engine:
    def __init__(self, run_dir: Path, workdir: Path, registry: RigRegistry) -> None:
        self.run_dir = Path(run_dir)
        self.workdir = Path(workdir)
        self.registry = registry
        # Load-continues-seq: a restart reuses the durable journal (B1); the in-memory
        # mirror keeps appending strictly-increasing seqs. This covers SERIAL restarts
        # only — parallel dispatch (step 3) needs a single append owner (DESIGN §6/N2).
        self.journal = Journal.load(self.run_dir)
        self.run_id = self.run_dir.name
        # A snapshot of durable state at load time — the resume source of truth. It is
        # deliberately NOT updated as this run journals: a leaf executes once, and a
        # loop re-runs its node_ids each iteration, so live-updating would wrongly skip.
        self._state = _fold(self.journal.events())

    def run_epoch(self, tree: Expr, epoch: int) -> None:
        """Admit the tree, (re-)enter the epoch boundary, execute, close.

        Fully-closed epoch -> no-op. An already-opened-but-unclosed epoch is RESUMED:
        no second `opened` boundary, completed nodes are skipped, only not-yet-resulted
        nodes run (B1). A fresh epoch opens normally.
        """
        assign_node_ids(tree)
        if self._state.epoch_closed(epoch):
            return
        if not self._state.epoch_opened(epoch):
            self.journal.append(
                Boundary(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=tree.node_id,
                    phase="opened",
                    expr=tree.model_dump(),
                )
            )
        self._exec(tree, epoch)
        self.journal.append(
            Boundary(
                run_id=self.run_id,
                epoch=epoch,
                node_id=tree.node_id,
                phase="closed",
                reason="done",
            )
        )

    def _exec(self, node: Expr, epoch: int, floor_seq: int = -1) -> None:
        """Execute a node, skipping leaf work already durable above `floor_seq`.

        `floor_seq` is the resume frontier: durable state at or below it does NOT
        satisfy resume. Top-level nodes use -1 (any durable state counts). A loop's
        resumed-partial iteration uses its last `loop_iter` seq, so inner-node state
        from a COMPLETED iteration cannot masquerade as durable for the partial one (B4).
        """
        if isinstance(node, (Do, Inplace)) and self._is_durable(node, epoch, floor_seq):
            return
        if isinstance(node, Seq):
            for child in node.children:  # strict order
                self._exec(child, epoch, floor_seq)
        elif isinstance(node, Dispatch):
            # Unordered-parallel by contract; the PoC executes serially (real
            # parallelism is ladder step 3). Use Seq when order matters.
            for child in node.children:
                self._exec(child, epoch, floor_seq)
        elif isinstance(node, Loop):
            self._exec_loop(node, epoch)
        elif isinstance(node, Do):
            self._exec_do(node, epoch)
        elif isinstance(node, Inplace):
            self._exec_inplace(node, epoch)
        else:
            raise NotImplementedError(f"{node.kind} is not executable in the PoC")

    def _is_durable(self, node: Do | Inplace, epoch: int, floor_seq: int) -> bool:
        """Is this leaf's work durable (and thus skippable) on resume?

        A `do`/`inplace` with DECLARED FILE EFFECTS is durable only once the core's
        `integrated` (the committed diff) is journalled — a result without it is NOT
        durable (B5): the rig's claim alone never counts. An effectless node (no diff)
        is durable on its result alone. Anything at/below `floor_seq` is stale (B4).
        """
        key = (epoch, node.node_id)
        rseq = self._state.result_seq.get(key)
        if rseq is None or rseq <= floor_seq:
            return False
        result = self._state.results[key]
        if result.files:
            iseq = self._state.integrated_seq.get(key)
            return iseq is not None and iseq > floor_seq
        return True

    def _exec_loop(self, node: Loop, epoch: int) -> None:
        """Run `body` then check `until`; repeat until converged or `cap` iterations.

        `cap` is the one live rail (DESIGN §4). Cap-exhaustion is a *result* (ok=False),
        not a crash. Each iteration journals `loop_iter` so replay knows how many ran and
        which commit was last integrated (D5). On resume the body restarts from the last
        integrated iteration; the partial iteration re-runs only its not-yet-durable
        inner nodes (B4).
        """
        if node.until.kind != "cmd":
            raise NotImplementedError(
                "loop `until=flag` is planner-judged; lands with real planner integration"
            )
        key = (epoch, node.node_id)
        if key in self._state.results:
            return  # the loop already produced its final result — fully done
        resume_from = self._state.loop_iterations.get(key, 0)
        partial_floor = self._state.loop_last_iter_seq.get(key, -1)

        iterations = resume_from
        converged = False
        last_body: Result | None = None
        for i in range(resume_from, node.cap):
            floor = partial_floor if i == resume_from else _NO_RESUME
            before = len(self.journal.events())
            self._exec(node.body, epoch, floor)
            body_result = self._last_result_since(before)
            if body_result is not None:
                last_body = body_result
            commit = self._head_commit()
            converged = self._until_met(node.until)
            self.journal.append(
                LoopIter(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=node.node_id,
                    iteration=i,
                    commit=commit,
                    converged=converged,
                )
            )
            iterations = i + 1
            if converged:
                break
        # SF6: the loop's result IS the last integrated iteration's body artifact
        # (text/files); the convergence/cap disposition rides in the separate
        # `loop_status`, so a downstream combine consumes the artifact, not the prose.
        status = (
            f"converged after {iterations} iteration(s)"
            if converged
            else f"hit cap {node.cap} without convergence (partial progress preserved)"
        )
        self.journal.append(
            ResultEvent(
                run_id=self.run_id,
                epoch=epoch,
                node_id=node.node_id,
                ok=converged,
                text=last_body.text if last_body else "",
                files=last_body.files if last_body else [],
                exit_code=last_body.exit_code if last_body else None,
                loop_status=status,
            )
        )

    def _last_result_since(self, index: int) -> Result | None:
        """The most recent ResultEvent journalled since `index`, as a Result (or None)."""
        for ev in reversed(self.journal.events()[index:]):
            if isinstance(ev, ResultEvent):
                return Result(
                    text=ev.text,
                    files=ev.files,
                    ok=ev.ok,
                    exit_code=ev.exit_code,
                    outcome=ev.outcome,
                )
        return None

    def _until_met(self, until: Until) -> bool:
        """Run the `until` predicate cmd in the workdir; exit 0 means converged."""
        assert until.cmd is not None  # admission-time validator guarantees this
        return subprocess.run(until.cmd, shell=True, cwd=self.workdir).returncode == 0

    def _head_commit(self) -> str | None:
        """The workdir's current HEAD sha, or None if it has no commits yet."""
        proc = self._git("rev-parse", "HEAD")
        return proc.stdout.strip() if proc.returncode == 0 else None

    def _exec_do(self, node: Do, epoch: int) -> None:
        prompt = self._materialize_ctx(node, epoch)
        self.journal.append(
            Dispatched(
                run_id=self.run_id,
                epoch=epoch,
                node_id=node.node_id,
                rig=node.rig.name,
                task=node.task,
                workdir=str(self.workdir),
            )
        )
        if prompt is None:
            # An unresolvable ctx ref is a failed RESULT, not a crash (SF2).
            self._journal_result(
                node.node_id,
                epoch,
                Result(text=f"unresolved ctx for {node.node_id}", ok=False, outcome="failed"),
            )
            return
        rig = self.registry.resolve(node.rig.name)
        try:
            result = rig.run(prompt, self.workdir)
        except Exception as exc:  # SF3: a rig exception never escapes after `dispatched`
            self._journal_result(
                node.node_id,
                epoch,
                Result(text=f"rig raised: {exc}", ok=False, outcome="failed"),
            )
            return
        if not result.ok:
            self._journal_result(node.node_id, epoch, result)
            return
        # Core-mediated integration: stage + commit whatever the rig changed. The
        # committed diff (not the rig's word) is the durable record (B5).
        integ = self._integrate(None, f"do {node.node_id}")
        if integ.status == "failed":  # SF1: git failure -> journalled failed result
            self._journal_result(
                node.node_id,
                epoch,
                Result(text=f"do integration failed:\n{integ.stderr}", ok=False, outcome="failed"),
            )
            return
        # Result first, THEN integrated: a torn tail leaves an effectful result without
        # its `integrated`, which _is_durable correctly reads as NOT durable.
        self._journal_result(
            node.node_id,
            epoch,
            Result(
                text=result.text,
                files=integ.paths,
                ok=True,
                exit_code=result.exit_code,
                outcome=result.outcome,
            ),
        )
        if integ.status == "committed":
            self.journal.append(
                Integrated(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=node.node_id,
                    commit=integ.commit or "",
                    paths=integ.paths,
                )
            )

    def _materialize_ctx(self, node: Do, epoch: int) -> str | None:
        """Append declared `ctx` to the prompt; None if any ref is unresolvable (SF2).

        kind=file -> the file's content under a header; kind=node -> the referenced
        node's journalled result text (resolved from the journal at exec time).
        """
        if not node.ctx:
            return node.task
        parts = [node.task]
        for ref in node.ctx:
            block = self._resolve_ctx(ref, epoch)
            if block is None:
                return None
            parts.append(block)
        return "\n\n".join(parts)

    def _resolve_ctx(self, ref: CtxRef, epoch: int) -> str | None:
        if ref.kind == "file":
            target = self.workdir / ref.ref
            try:
                content = target.read_text(encoding="utf-8")
            except OSError:
                return None
            return f"## Context: file {ref.ref}\n{content}"
        # kind == "node": the referenced node's journalled result text, this epoch.
        text = self._journalled_result_text(epoch, ref.ref)
        if text is None:
            return None
        return f"## Context: node {ref.ref}\n{text}"

    def _journalled_result_text(self, epoch: int, node_id: str) -> str | None:
        for ev in reversed(self.journal.events()):
            if isinstance(ev, ResultEvent) and ev.epoch == epoch and ev.node_id == node_id:
                return ev.text
        return None

    def _exec_inplace(self, node: Inplace, epoch: int) -> None:
        self.journal.append(
            Dispatched(
                run_id=self.run_id,
                epoch=epoch,
                node_id=node.node_id,
                task=f"inplace: {len(node.edits)} edit(s)",
                workdir=str(self.workdir),
            )
        )
        if not node.edits:
            # SF1: an empty inplace is a no-op ok result with NO git calls.
            self._journal_result(
                node.node_id, epoch, Result(text="inplace: no edits", files=[], ok=True)
            )
            return
        paths: list[str] = []
        for edit in node.edits:
            self._reject_admin_path(edit.path)
            target = (self.workdir / edit.path).resolve()
            if not target.is_relative_to(self.workdir.resolve()):
                raise ValueError(f"inplace edit escapes workdir: {edit.path}")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(edit.content, encoding="utf-8")
            paths.append(edit.path)
        integ = self._integrate(paths, f"inplace {node.node_id}")
        if integ.status == "failed":  # SF1
            self._journal_result(
                node.node_id,
                epoch,
                Result(
                    text=f"inplace integration failed:\n{integ.stderr}",
                    ok=False,
                    outcome="failed",
                ),
            )
            return
        if integ.status == "committed":
            self.journal.append(
                Integrated(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=node.node_id,
                    commit=integ.commit or "",
                    paths=paths,
                )
            )
        self._journal_result(
            node.node_id,
            epoch,
            Result(
                text=f"wrote {', '.join(paths)}",
                files=paths,
                ok=integ.status == "committed",
            ),
        )

    def _reject_admin_path(self, edit_path: str) -> None:
        """Reject writes to the worktree's Git admin path (N1).

        A `.git` component is refused outright; in a LINKED worktree `.git` is a file
        pointing at the real gitdir, so the resolved absolute gitdir (and anything under
        it) is refused too — overwriting it would corrupt the worktree before the core
        ever runs Git.
        """
        if ".git" in Path(edit_path).parts:
            raise ValueError(f"inplace edit targets a git admin path: {edit_path}")
        absgit = self._git("rev-parse", "--absolute-git-dir")
        if absgit.returncode == 0 and absgit.stdout.strip():
            gitdir = Path(absgit.stdout.strip()).resolve()
            target = (self.workdir / edit_path).resolve()
            if target == gitdir or target.is_relative_to(gitdir):
                raise ValueError(f"inplace edit targets a git admin path: {edit_path}")

    def _git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=self.workdir, capture_output=True, text=True
        )

    def _integrate(self, declared: list[str] | None, message: str) -> _Integration:
        """Core-mediated commit. `declared=None` -> all worktree changes (a `do`); a
        path list -> ONLY those declared paths, even if other changes are staged (an
        `inplace`, via git's `--`-scoped pathspec commit). Git failures return
        status="failed" with stderr; never raises (SF1)."""
        if declared is None:
            add = self._git("add", "-A", "--", ".")
        else:
            add = self._git("add", "--", *declared)
        if add.returncode != 0:
            return _Integration("failed", stderr=add.stderr)
        if declared is None:
            diff = self._git("diff", "--cached", "--quiet")
        else:
            diff = self._git("diff", "--cached", "--quiet", "--", *declared)
        if diff.returncode == 0:  # nothing staged for our scope -> no-op
            return _Integration("noop")
        if declared is None:
            commit = self._git("commit", "-q", "-m", message)
        else:
            # Pathspec commit: commits ONLY these paths, leaving any other staged
            # changes staged-and-uncommitted (reviewer B6, scenario A).
            commit = self._git("commit", "-q", "-m", message, "--", *declared)
        if commit.returncode != 0:
            return _Integration("failed", stderr=commit.stderr)
        sha = self._git("rev-parse", "HEAD").stdout.strip()
        if declared is None:
            changed = self._git(
                "diff-tree", "--no-commit-id", "--name-only", "-r", "--root", "HEAD"
            ).stdout.split()
        else:
            changed = list(declared)
        return _Integration("committed", commit=sha, paths=changed)

    def _journal_result(self, node_id: str, epoch: int, result: Result) -> None:
        self.journal.append(
            ResultEvent(
                run_id=self.run_id,
                epoch=epoch,
                node_id=node_id,
                ok=result.ok,
                text=result.text,
                files=result.files,
                exit_code=result.exit_code,
                outcome=result.outcome,
            )
        )


class ReplayState:
    """Per-(epoch, node_id) run state folded from the journal.

    `(epoch, node_id)` — NOT node_id alone — is the cross-epoch join key: a reopened
    epoch's node must never inherit an earlier epoch's result (B3). Epoch open/closed
    folds to the LATEST boundary event for that epoch.
    """

    def __init__(self) -> None:
        self.results: dict[tuple[int, str], Result] = {}
        self.result_seq: dict[tuple[int, str], int] = {}
        self.integrated: dict[tuple[int, str], list[str]] = {}
        self.integrated_seq: dict[tuple[int, str], int] = {}
        self.dispatched: set[tuple[int, str]] = set()
        # Per loop (epoch, node_id): completed-iteration count, last integrated commit,
        # and the seq of the last loop_iter (the B4 partial-iteration resume frontier).
        self.loop_iterations: dict[tuple[int, str], int] = {}
        self.loop_last_commit: dict[tuple[int, str], str | None] = {}
        self.loop_last_iter_seq: dict[tuple[int, str], int] = {}
        self._epoch_phase: dict[int, str] = {}

    def epoch_closed(self, epoch: int) -> bool:
        return self._epoch_phase.get(epoch) == "closed"

    def epoch_opened(self, epoch: int) -> bool:
        return epoch in self._epoch_phase


def _fold(events: list[Event]) -> ReplayState:
    """Fold a journal event list into per-(epoch, node_id) state (the resume/dashboard fold)."""
    state = ReplayState()
    for ev in events:
        key = (ev.epoch, ev.node_id)
        if isinstance(ev, Dispatched):
            state.dispatched.add(key)
        elif isinstance(ev, ResultEvent):
            state.results[key] = Result(
                text=ev.text,
                files=ev.files,
                ok=ev.ok,
                exit_code=ev.exit_code,
                outcome=ev.outcome,
            )
            state.result_seq[key] = ev.seq
        elif isinstance(ev, Integrated):
            state.integrated[key] = ev.paths
            state.integrated_seq[key] = ev.seq
        elif isinstance(ev, LoopIter):
            state.loop_iterations[key] = ev.iteration + 1
            state.loop_last_commit[key] = ev.commit
            state.loop_last_iter_seq[key] = ev.seq
        elif isinstance(ev, Boundary):
            state._epoch_phase[ev.epoch] = ev.phase  # latest boundary wins (B3)
    return state


def replay(run_dir: Path) -> ReplayState:
    """Reconstruct run state from the ndjson alone — the single resume/dashboard path."""
    return _fold(Journal.load(run_dir).events())
