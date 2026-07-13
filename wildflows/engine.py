"""The minimal engine (ladder step 2): epoch lifecycle + expression traversal.

Authority is split: admission proves the tree (`admission.admit_epoch`), the journal is
the single append owner that folds every event into one live `RunProjection`, and all
git/path/containment effects live behind `Workspace`. What stays here is orchestration —
open/resume/close an epoch, walk the expression, run each primitive, and record its
result. Effects are core-mediated: after a rig runs, the CORE stages + commits and emits
`integrated`; the durable record is the committed diff, never the rig's claim.

Resume = fold-the-journal: `run_epoch` re-enters an already-opened epoch without
re-executing durable nodes, and `replay` reconstructs the same projection from the
ndjson alone. Executable: do, inplace, seq, dispatch (serial in the PoC), and loop
(cmd predicate); admission rejects the not-yet-executable kinds before any event.
"""
from __future__ import annotations

from pathlib import Path

from wildflows.admission import admit_epoch
from wildflows.events import (
    Boundary,
    Dispatched,
    Integrated,
    LoopIter,
    ResultEvent,
)
from wildflows.expr import CtxRef, Dispatch, Do, Expr, Inplace, Loop, Seq, Until
from wildflows.journal import Journal
from wildflows.projection import Floor, RunProjection, replay
from wildflows.result import Result
from wildflows.rig import RigRegistry
from wildflows.workspace import Workspace

__all__ = ["Engine", "replay", "RunProjection"]


class Engine:
    def __init__(self, run_dir: Path, workdir: Path, registry: RigRegistry) -> None:
        self.run_dir = Path(run_dir)
        self.registry = registry
        # Load-continues-seq: a restart reuses the durable journal; its live
        # projection is folded from disk on load and updated on every append. This covers
        # SERIAL restarts only — parallel dispatch (step 3) needs the single append owner
        # this journal already is (DESIGN §6).
        self.journal = Journal.load(self.run_dir)
        self.run_id = self.run_dir.name
        self.ws = Workspace(workdir, self.run_dir)

    @property
    def workdir(self) -> Path:
        return self.ws.workdir

    @property
    def _proj(self) -> RunProjection:
        return self.journal.projection

    def run_epoch(self, tree: Expr, epoch: int) -> None:
        """Admit the tree, (re-)enter the epoch boundary, execute, close.

        Fully-closed epoch -> no-op. An already-opened-but-unclosed epoch is RESUMED:
        no second `opened` boundary, durable nodes are skipped, only not-yet-durable
        nodes run. A fresh epoch opens normally. Admission runs BEFORE any event, so
        an inadmissible tree never opens an incomplete epoch (DESIGN §3).
        """
        tree = admit_epoch(tree, epoch, self._proj, self.registry)
        if self._proj.epoch_closed(epoch):
            return
        if not self._proj.epoch_opened(epoch):
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

    def _exec(self, node: Expr, epoch: int, floor: Floor = -1) -> None:
        """Execute a node, skipping leaf work already durable within `floor`'s scope.

        `floor` is the resume frontier: durable state at or below it does NOT satisfy
        resume; `None` (a fresh loop iteration) never satisfies it. Top-level nodes use
        -1 (any durable state counts). A loop's resumed-partial iteration uses its last
        `loop_iter` seq, so inner-node state from a COMPLETED iteration cannot masquerade
        as durable for the partial one.
        """
        if isinstance(node, (Do, Inplace)):
            if self._proj.resume_action((epoch, node.node_id), floor) == "done":
                return
        if isinstance(node, Seq):
            for child in node.children:  # strict order
                self._exec(child, epoch, floor)
        elif isinstance(node, Dispatch):
            # Unordered-parallel by contract; the PoC executes serially (real
            # parallelism is ladder step 3). Use Seq when order matters.
            for child in node.children:
                self._exec(child, epoch, floor)
        elif isinstance(node, Loop):
            self._exec_loop(node, epoch, floor)
        elif isinstance(node, Do):
            self._exec_do(node, epoch, floor)
        elif isinstance(node, Inplace):
            self._exec_inplace(node, epoch, floor)

    def _exec_loop(self, node: Loop, epoch: int, floor: Floor) -> None:
        """Run `body` then check `until`; repeat until converged or `cap` iterations.

        `cap` is the one live rail (DESIGN §4). Cap-exhaustion is a *result* (ok=False),
        not a crash. Each iteration journals `loop_iter` so replay knows how many ran and
        which commit was last integrated (D5). On resume the body restarts from the last
        integrated iteration; the partial iteration re-runs only its not-yet-durable
        inner nodes. A fresh iteration (floor `None`) never resumes.
        """
        key = (epoch, node.node_id)
        fresh = floor is None
        proj = self._proj
        state = proj.node(key)
        if not fresh and proj.has_result(key):
            return  # the loop already produced its final result — fully done
        resume_from = 0 if fresh else state.loop_iterations
        partial_floor: Floor = -1 if fresh else state.loop_last_iter_seq
        last_converged = False if fresh else state.loop_converged
        last_body: Result | None = None if fresh else state.loop_last_body

        # A crash BETWEEN the last loop_iter and the loop's final ResultEvent must not
        # re-run a body that already converged or hit the cap: reconstruct the result
        # from the journalled last-body artifact and emit it straight away.
        if resume_from > 0 and (last_converged or resume_from >= node.cap):
            self._finish_loop(
                node, epoch, last_body, iterations=resume_from, converged=last_converged
            )
            return

        iterations = resume_from
        converged = False
        for i in range(resume_from, node.cap):
            body_floor: Floor = partial_floor if i == resume_from else None
            before = self.journal.n_events
            self._exec(node.body, epoch, body_floor)
            body_result = proj.last_result_since(before)
            if body_result is not None:
                last_body = body_result
            commit = self.ws.head_commit()
            converged = self.ws.run_predicate(self._until_cmd(node.until))
            self.journal.append(
                LoopIter(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=node.node_id,
                    iteration=i,
                    commit=commit,
                    converged=converged,
                    body_text=last_body.text if last_body else "",
                    body_files=last_body.files if last_body else [],
                    body_exit_code=last_body.exit_code if last_body else None,
                )
            )
            iterations = i + 1
            if converged:
                break
        self._finish_loop(node, epoch, last_body, iterations=iterations, converged=converged)

    def _finish_loop(
        self, node: Loop, epoch: int, last_body: Result | None, *, iterations: int, converged: bool
    ) -> None:
        # The loop's result IS the last integrated iteration's body artifact (text/files);
        # the convergence/cap disposition rides in the separate `loop_status`, so a
        # downstream combine consumes the artifact, not the prose.
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

    def _until_cmd(self, until: Until) -> str:
        assert until.cmd is not None  # admission guarantees a cmd predicate has a command
        return until.cmd

    def _exec_do(self, node: Do, epoch: int, floor: Floor = -1) -> None:
        # A prior session may have committed this node then died before journalling.
        # Reconcile from the marked commit instead of re-running the rig (top-level only;
        # a loop body's per-iteration marker is owned by the loop fold, not this scan).
        if floor == -1 and self._reconcile_committed(node.node_id, epoch, "do"):
            return
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
            # An unresolvable/escaping ctx ref is a failed RESULT, not a crash.
            self._journal_result(
                node.node_id,
                epoch,
                Result(text=f"unresolved ctx for {node.node_id}", ok=False, outcome="failed"),
            )
            return
        pre_head = self.ws.head_commit()  # snapshot to attribute rig-made commits
        try:
            rig = self.registry.resolve(node.rig.name)
            result = rig.run(prompt, self.workdir)
        except Exception as exc:  # a rig exception never escapes after `dispatched`
            diff_path = self.ws.capture_and_reset_dirty(f"e{epoch}-{node.node_id}.diff")
            self._journal_result(
                node.node_id,
                epoch,
                Result(text=self._fail_text(f"rig raised: {exc}", diff_path), ok=False,
                       outcome="failed"),
            )
            return
        if not result.ok:
            # A failed rig's working-tree changes are captured verbatim to the run log dir
            # and the workdir is reset to HEAD, so no LATER node can stage + claim the leak.
            diff_path = self.ws.capture_and_reset_dirty(f"e{epoch}-{node.node_id}.diff")
            self._journal_result(
                node.node_id,
                epoch,
                Result(text=self._fail_text(result.text, diff_path), ok=False,
                       files=result.files, exit_code=result.exit_code, outcome=result.outcome),
            )
            return
        # The senior/script contract legitimately commits its OWN work; the core RECORDS
        # those rig-made commits (pre_head..HEAD) as this node's integration, then
        # integrates any REMAINING dirty state. The committed diff is the durable record.
        pending: list[Integrated] = []
        integrated_paths: list[str] = []
        post_head = self.ws.head_commit()
        if post_head is not None and post_head != pre_head:
            rig_paths = self.ws.paths_in_range(pre_head, post_head)
            integrated_paths += rig_paths
            pending.append(Integrated(
                run_id=self.run_id, epoch=epoch, node_id=node.node_id,
                commit=post_head, paths=rig_paths,
            ))
        integ = self.ws.integrate(None, self._commit_msg("do", node.node_id, epoch))
        if integ.status == "failed":  # git failure -> journalled failed result
            self._journal_result(
                node.node_id,
                epoch,
                Result(text=f"do integration failed:\n{integ.stderr}", ok=False, outcome="failed"),
            )
            return
        if integ.status == "committed":
            integrated_paths += integ.paths
            pending.append(Integrated(
                run_id=self.run_id, epoch=epoch, node_id=node.node_id,
                commit=integ.commit or "", paths=integ.paths,
            ))
        # Result first, THEN integrated: a torn tail leaves an effectful result without
        # its `integrated`, which resume_action correctly reads as NOT durable; the marker
        # then reconciles the orphaned commit on the next resume.
        self._journal_result(
            node.node_id,
            epoch,
            Result(
                text=result.text,
                files=integrated_paths,
                ok=True,
                exit_code=result.exit_code,
                outcome=result.outcome,
            ),
        )
        for ev in pending:
            self.journal.append(ev)

    def _materialize_ctx(self, node: Do, epoch: int) -> str | None:
        """Append declared `ctx` to the prompt; None if any ref is unresolvable.

        kind=file -> the file's content under a header; kind=node -> the referenced
        node's journalled result text (resolved from the projection at exec time).
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
            # Containment guard identical to `inplace`: a file ctx must resolve INSIDE the
            # workdir (lexical escapes are rejected at admission; a symlink pointing out is
            # a failed result here — admission cannot resolve symlinks).
            content = self.ws.read_contained_file(ref.ref)
            if content is None:
                return None
            return f"## Context: file {ref.ref}\n{content}"
        # kind == "node": the referenced node's journalled result text, this epoch.
        text = self._proj.result_text((epoch, ref.ref))
        if text is None:
            return None
        return f"## Context: node {ref.ref}\n{text}"

    def _exec_inplace(self, node: Inplace, epoch: int, floor: Floor = -1) -> None:
        if floor == -1 and self._reconcile_committed(node.node_id, epoch, "inplace"):
            return  # a marked commit from a crashed prior session — do not re-apply
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
            # An empty inplace is a no-op ok result with NO git calls.
            self._journal_result(
                node.node_id, epoch, Result(text="inplace: no edits", files=[], ok=True)
            )
            return
        paths: list[str] = []
        for edit in node.edits:
            target = self.ws.resolve_safe_path(edit.path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(edit.content, encoding="utf-8")
            paths.append(edit.path)
        integ = self.ws.integrate(paths, self._commit_msg("inplace", node.node_id, epoch))
        if integ.status == "failed":
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
        if integ.status == "noop":
            # An already-identical edit produced no diff: journal a DURABLE no-op (ok=True,
            # empty file list) so resume_action accepts it on its own result and resume
            # never re-applies it. DESIGN §5: "empty/no-diff inplace is a durable no-op."
            self._journal_result(
                node.node_id,
                epoch,
                Result(text=f"inplace: no diff (already applied) for {', '.join(paths)}",
                       files=[], ok=True),
            )
            return
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
            Result(text=f"wrote {', '.join(paths)}", files=paths, ok=True),
        )

    def _commit_msg(self, kind: str, node_id: str, epoch: int) -> str:
        """A commit message carrying a machine-parsable reconciliation marker.

        The marker `wf:<run_id>:<epoch>:<node_id>` lets a resumed run find a commit the
        core made just before it crashed (commit succeeded, journal write did not) and
        retro-journal it instead of re-executing.
        """
        return f"{kind} {node_id}\n\nwf:{self.run_id}:{epoch}:{node_id}"

    def _reconcile_committed(self, node_id: str, epoch: int, kind: str) -> bool:
        """If a marked commit for this node already exists (a crash after the core
        committed but before it journalled), retro-journal `integrated` + `result` from
        that commit and report the node done — never re-run its effect."""
        marker = f"wf:{self.run_id}:{epoch}:{node_id}"
        sha = self.ws.find_marked_commit(marker)
        if sha is None:
            return False
        paths = self.ws.paths_in_commit(sha)
        self.journal.append(
            Integrated(run_id=self.run_id, epoch=epoch, node_id=node_id, commit=sha, paths=paths)
        )
        self._journal_result(
            node_id, epoch,
            Result(text=f"{kind} reconciled from marked commit {sha}", files=paths, ok=True),
        )
        return True

    def _fail_text(self, base: str, diff_path: Path | None) -> str:
        if diff_path is None:
            return base
        return f"{base}\n[dirty working-tree diff captured: {diff_path}]"

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
