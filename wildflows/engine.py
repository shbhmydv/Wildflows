"""The minimal engine (ladder step 2): epoch lifecycle + expression traversal.

Authority is split: admission proves the tree (`admission.admit_epoch`), the journal is
the single append owner that folds every event into one live `RunProjection`, and ALL
git/path/containment effects live behind `WorkspaceEffects` — the engine issues zero git
commands. What stays here is orchestration: open/resume/close an epoch, walk the
expression, run each primitive in its lease, and let `WorkspaceEffects` finalize the
effect while `CompletionRecorder` records the one canonical event ordering. Effects are
core-mediated: the durable record is the committed diff (an `IntegrationReceipt`), never
the rig's claim.

`_exec` returns an `ExecutionOutcome` — a reference into the journalled projection, not a
payload. A leaf carries its result key; `seq`/`dispatch` carry position-indexed child
references; a loop reads its body's outcome through that reference (no journal re-scan).

Resume = fold-the-journal: `run_epoch` re-enters an already-opened epoch without
re-executing durable nodes, and `replay` reconstructs the same projection from the
ndjson alone. Executable: do, inplace, seq, dispatch (serial in the PoC), and loop
(cmd predicate); admission rejects the not-yet-executable kinds before any event.
"""
from __future__ import annotations

from pathlib import Path

from wildflows.admission import admit_epoch
from wildflows.events import Boundary, Dispatched, LoopIter
from wildflows.expr import CtxRef, Dispatch, Do, Expr, Inplace, Loop, Seq, Until
from wildflows.journal import Journal
from wildflows.projection import ExecutionOutcome, Floor, RunProjection, replay
from wildflows.result import CommitReceipt, IntegrationReceipt, Result
from wildflows.rig import RigRegistry
from wildflows.workspace import CompletionRecorder, Lease, WorkspaceEffects

__all__ = ["Engine", "replay", "RunProjection"]


class Engine:
    def __init__(self, run_dir: Path, workdir: Path, registry: RigRegistry) -> None:
        self.run_dir = Path(run_dir)
        self.registry = registry
        # Load-continues-seq: a restart reuses the durable journal; its live projection is
        # folded from disk on load and updated on every append. Serial restarts only —
        # parallel dispatch (step 3) needs the single append owner this journal is (§6).
        self.journal = Journal.load(self.run_dir)
        self.run_id = self.run_dir.name
        self.ws = WorkspaceEffects(workdir, self.run_dir)
        self.rec = CompletionRecorder(self.journal, self.run_id)

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
        nodes run. A fresh epoch opens normally. Admission runs BEFORE any event, so an
        inadmissible tree never opens an incomplete epoch (DESIGN §3).
        """
        tree = admit_epoch(tree, epoch, self._proj, self.registry)
        if self._proj.epoch_closed(epoch):
            return
        if not self._proj.epoch_opened(epoch):
            self.journal.append(Boundary(
                run_id=self.run_id, epoch=epoch, node_id=tree.node_id,
                phase="opened", expr=tree.model_dump(),
            ))
        self._exec(tree, epoch)
        self.journal.append(Boundary(
            run_id=self.run_id, epoch=epoch, node_id=tree.node_id, phase="closed", reason="done",
        ))

    def _exec(self, node: Expr, epoch: int, floor: Floor = -1) -> ExecutionOutcome:
        """Execute a node, skipping leaf work already durable within `floor`'s scope.

        Returns an `ExecutionOutcome` referencing the journalled result(s). `floor` is the
        resume frontier: durable state at or below it does NOT satisfy resume; `None` (a
        fresh loop iteration) never satisfies it. Top-level nodes use -1.
        """
        key = (epoch, node.node_id)
        if isinstance(node, (Do, Inplace)) and self._proj.resume_action(key, floor) == "done":
            return ExecutionOutcome(key=key)
        if isinstance(node, Seq):  # strict order
            return ExecutionOutcome(
                children=tuple(self._exec(c, epoch, floor) for c in node.children)
            )
        if isinstance(node, Dispatch):
            # Unordered-parallel by contract; the PoC executes serially (real parallelism
            # is step 3). Kept a DISTINCT branch from Seq: identical loops, opposite
            # contracts (strict order vs unordered concurrency) — merging hides that.
            return ExecutionOutcome(
                children=tuple(self._exec(c, epoch, floor) for c in node.children)
            )
        if isinstance(node, Loop):
            return self._exec_loop(node, epoch, floor)
        if isinstance(node, Do):
            return self._exec_do(node, epoch, floor)
        if isinstance(node, Inplace):
            return self._exec_inplace(node, epoch, floor)
        return ExecutionOutcome(key=key)  # admission rejects any other kind before here

    def _exec_loop(self, node: Loop, epoch: int, floor: Floor) -> ExecutionOutcome:
        """Run `body` then check `until`; repeat until converged or `cap` iterations.

        `cap` is the one live rail (DESIGN §4). Cap-exhaustion is a *result* (outcome
        `failed`), not a crash. Each iteration journals `loop_iter` so replay knows how
        many ran and which commit was last integrated (D5). On resume the body restarts
        from the last integrated iteration; the partial iteration re-runs only its
        not-yet-durable inner nodes. A fresh iteration (floor `None`) never resumes.
        """
        key = (epoch, node.node_id)
        proj = self._proj
        lstate = proj.node(key)
        # The loop's final result is durable for THIS invocation only if it was recorded
        # within scope (result_seq > floor). A nested inner loop that produced a result in a
        # PRIOR outer iteration (result_seq <= the resumed floor) is NOT done — it re-runs.
        if floor is not None and lstate.result is not None and lstate.result_seq > floor:
            return ExecutionOutcome(key=key)
        # Floor-scoped resume: only loop_iters after `floor` belong to this invocation. A
        # FRESH iteration (floor None) counts none and re-runs the whole body — a nested
        # inner loop never treats a prior outer iteration's iters/result as its own
        # (hand-9, LOOP-OUTCOME-TOTALITY nested-loop floor bug).
        resume_from, partial_floor, last_converged, last_body = proj.loop_resume(key, floor)

        # A crash BETWEEN the last loop_iter and the loop's final ResultEvent must not
        # re-run a body that already converged or hit the cap: reconstruct from the
        # journalled last-body reference and emit the final result straight away.
        if resume_from > 0 and (last_converged or resume_from >= node.cap):
            self._finish_loop(node, epoch, last_body, iterations=resume_from,
                              converged=last_converged)
            return ExecutionOutcome(key=key)

        iterations = resume_from
        converged = False
        for i in range(resume_from, node.cap):
            body_floor: Floor = partial_floor if i == resume_from else None
            outcome = self._exec(node.body, epoch, body_floor)
            # Admission's totality rule (finding 3a) guarantees the body's last positional
            # child chain ends in a result-producing leaf, so `result_key()` is TOTAL and
            # the body always has a journalled result — the loop_iter reference is never
            # None on the live path (the None fallback in the projection is legacy-only).
            body_key = outcome.result_key()
            assert body_key is not None, "admission totality guarantees a resultful body"
            body_result = proj.result(body_key)
            if body_result is not None:
                last_body = body_result
            body_seq = proj.node(body_key).result_seq
            converged = self.ws.run_predicate(self._until_cmd(node.until))
            self.journal.append(LoopIter(
                run_id=self.run_id, epoch=epoch, node_id=node.node_id,
                iteration=i, commit=self.ws.head_commit(), converged=converged,
                body_result_seq=body_seq,
            ))
            iterations = i + 1
            if converged:
                break
        self._finish_loop(node, epoch, last_body, iterations=iterations, converged=converged)
        return ExecutionOutcome(key=key)

    def _finish_loop(
        self, node: Loop, epoch: int, last_body: Result | None, *, iterations: int, converged: bool
    ) -> None:
        # The loop's result IS the last integrated iteration's body artifact (text/files);
        # the convergence/cap disposition rides in the SEPARATE loop_status, so a
        # downstream combine consumes the artifact, not the prose.
        status = (
            f"converged after {iterations} iteration(s)" if converged
            else f"hit cap {node.cap} without convergence (partial progress preserved)"
        )
        result = Result(
            text=last_body.text if last_body else "",
            files=last_body.files if last_body else [],
            exit_code=last_body.exit_code if last_body else None,
            outcome="ok" if converged else "failed",
        )
        self.rec.record_loop_result((epoch, node.node_id), result, status)

    def _until_cmd(self, until: Until) -> str:
        assert until.cmd is not None  # admission guarantees a cmd predicate has a command
        return until.cmd

    def _recover_committed_attempt(self, key: tuple[int, str], floor: Floor) -> bool:
        """Two-boundary provenance recovery (hand-9, PROVENANCE-RANGE). The ONLY torn
        window that is recoverable as success is a durable OK `result` whose `integrated`
        was lost: the result stamped a `post_head` completion certificate, so the commits
        in EXACTLY `pre_head..post_head` are that attempt's and the receipt is reconstructed
        and retro-journalled — never re-run, never `..HEAD` (an operator commit above
        `post_head` is out of range by construction). Returns True if recovered.

        A DISPATCHED-ONLY tail (no `result`) has NO completion proof — a mid-rig checkpoint
        commit is not a certificate — so it is NEVER recovered as success: the caller cleans
        its leftover dirt and RE-RUNS the node. A durable FAILED result is likewise not a
        recovery (its effect was reverted). The `wf:` commit marker survives as forensic
        metadata only.
        """
        if floor != -1:
            return False
        node = self._proj.node(key)
        if node.result is None or not node.result.ok:
            return False  # dispatched-only or failed: no completion certificate to recover
        receipt = self.ws.reconstruct_receipt(node.dispatched_pre_head, node.result_post_head)
        if not receipt.commits:
            return False  # effectless result — durable on its own; resume_action handles it
        self.rec.record_integrated(key, receipt)
        return True

    def _exec_do(self, node: Do, epoch: int, floor: Floor = -1) -> ExecutionOutcome:
        key = (epoch, node.node_id)
        if self._recover_committed_attempt(key, floor):
            return ExecutionOutcome(key=key)
        lease = self._open_lease_or_fail(key, floor)
        if lease is None:
            return ExecutionOutcome(key=key)
        prompt = self._materialize_ctx(node, epoch)
        self.journal.append(Dispatched(
            run_id=self.run_id, epoch=epoch, node_id=node.node_id,
            rig=node.rig.name, task=node.task, workdir=str(self.workdir),
            pre_head=lease.pre_head,
        ))
        if prompt is None:  # an unresolvable/escaping ctx ref is a failed RESULT
            self.rec.record_result(key, Result(
                text=f"unresolved ctx for {node.node_id}", outcome="failed"),
                post_head=self.ws.head_commit())
            return ExecutionOutcome(key=key)
        diff_name = f"e{epoch}-{node.node_id}.diff"
        try:
            result = self.registry.resolve(node.rig.name).run(prompt, self.workdir)
        except Exception as exc:  # a rig exception never escapes after `dispatched`
            diff_path = self.ws.finalize_failure(lease, diff_name)
            self.rec.record_result(key, Result(
                text=self._fail_text(f"rig raised: {exc}", diff_path), outcome="failed"),
                post_head=self.ws.head_commit())
            return ExecutionOutcome(key=key)
        if not result.ok:
            # The failed rig's effects (incl its own commits) are reverted + captured.
            diff_path = self.ws.finalize_failure(lease, diff_name)
            self.rec.record_result(key, Result(
                text=self._fail_text(result.text, diff_path),
                files=result.files, exit_code=result.exit_code, outcome=result.outcome),
                post_head=self.ws.head_commit())
            return ExecutionOutcome(key=key)
        integ = self.ws.finalize_do_success(lease, self._commit_msg("do", node.node_id, epoch))
        if not integ.ok:
            # A git failure integrating a SUCCESSFUL rig's dirty state is a failed
            # transaction, not a bare error: revert + capture the leak through the same
            # failure path so no later node inherits it (hand-8, FAILURE-TRANSACTION).
            diff_path = self.ws.finalize_failure(lease, diff_name)
            self.rec.record_result(key, Result(
                text=self._fail_text(f"do integration failed:\n{integ.stderr}", diff_path),
                outcome="failed"), post_head=self.ws.head_commit())
            return ExecutionOutcome(key=key)
        # Result.files is the artifact list (== the effect paths in the shared-workdir
        # PoC); the receipt is the ownership record replay accumulates. `post_head` is the
        # completion certificate the torn-window receipt reconstruction is bounded by.
        self.rec.record_success(key, Result(
            text=result.text, files=integ.receipt.paths,
            exit_code=result.exit_code, outcome=result.outcome), integ.receipt,
            post_head=self.ws.head_commit())
        return ExecutionOutcome(key=key)

    def _open_lease_or_fail(self, key: tuple[int, str], floor: Floor) -> Lease | None:
        """Open a lease, first cleaning a dead dispatched-only attempt's leftover dirt on a
        top-level resume (hand-9). A refused lease (dirty tracked/index worktree) records a
        durable failed result and returns None — the honest serial-M1 rule."""
        if floor == -1 and self._proj.node(key).dispatched:
            self.ws.clean_dispatched_residue()
        lease = self.ws.open_lease(key)
        if lease is None:
            self.rec.record_result(key, Result(
                text="workdir has uncommitted tracked changes; lease refused",
                outcome="failed"), post_head=self.ws.head_commit())
        return lease

    def _exec_inplace(self, node: Inplace, epoch: int, floor: Floor = -1) -> ExecutionOutcome:
        key = (epoch, node.node_id)
        if self._recover_committed_attempt(key, floor):
            return ExecutionOutcome(key=key)
        lease = self._open_lease_or_fail(key, floor)
        if lease is None:
            return ExecutionOutcome(key=key)
        self.journal.append(Dispatched(
            run_id=self.run_id, epoch=epoch, node_id=node.node_id,
            task=f"inplace: {len(node.edits)} edit(s)", workdir=str(self.workdir),
            pre_head=lease.pre_head,
        ))
        if not node.edits:  # an empty inplace is a no-op ok result with NO git calls
            self.rec.record_result(key, Result(text="inplace: no edits", files=[]),
                                   post_head=self.ws.head_commit())
            return ExecutionOutcome(key=key)
        # Track every write (path + the content it pre-existed with, None if created) so a
        # failure AFTER the first write rolls back to leave NO partial effect (hand-9,
        # INPLACE-TRANSACTIONAL).
        paths: list[str] = []
        writes: list[tuple[str, str | None]] = []
        try:
            for edit in node.edits:
                target = self.ws.resolve_safe_path(edit.path)
                original = target.read_text(encoding="utf-8") if target.is_file() else None
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(edit.content, encoding="utf-8")
                writes.append((edit.path, original))
                paths.append(edit.path)
        except ValueError as exc:
            # A symlink resolving outside the workdir can only be caught at write time
            # (admission cannot resolve symlinks); roll back earlier edits, then a durable
            # FAILED result — never an exception escaping after `dispatched`.
            self.ws.rollback_inplace(writes)
            self.rec.record_result(key, Result(
                text=f"inplace path rejected: {exc}", outcome="failed"),
                post_head=self.ws.head_commit())
            return ExecutionOutcome(key=key)
        integ = self.ws.integrate_declared(paths, self._commit_msg("inplace", node.node_id, epoch))
        if integ.status == "failed":
            self.ws.rollback_inplace(writes)  # no partial write survives a failed commit
            self.rec.record_result(key, Result(
                text=f"inplace integration failed:\n{integ.stderr}", outcome="failed"),
                post_head=self.ws.head_commit())
            return ExecutionOutcome(key=key)
        if integ.status == "noop":
            # An already-identical edit produced no diff: a DURABLE no-op (files=[]), so
            # resume never re-applies it (DESIGN §5).
            self.rec.record_result(key, Result(
                text=f"inplace: no diff (already applied) for {', '.join(paths)}", files=[]),
                post_head=self.ws.head_commit())
            return ExecutionOutcome(key=key)
        receipt = IntegrationReceipt(commits=[CommitReceipt(sha=integ.commit or "", paths=paths)])
        self.rec.record_success(key, Result(text=f"wrote {', '.join(paths)}", files=paths),
                                receipt, post_head=self.ws.head_commit())
        return ExecutionOutcome(key=key)

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
            # workdir and not alias the gitdir (a symlink escape is a failed result here —
            # admission cannot resolve symlinks).
            content = self.ws.read_contained_file(ref.ref)
            if content is None:
                return None
            return f"## Context: file {ref.ref}\n{content}"
        text = self._proj.result_text((epoch, ref.ref))  # kind == "node"
        if text is None:
            return None
        return f"## Context: node {ref.ref}\n{text}"

    def _marker(self, node_id: str, epoch: int) -> str:
        return f"wf:{self.run_id}:{epoch}:{node_id}"

    def _commit_msg(self, kind: str, node_id: str, epoch: int) -> str:
        """A commit message carrying the machine-parsable reconciliation marker, so a
        resumed run can find a commit the core made just before it crashed and
        retro-journal it instead of re-executing."""
        return f"{kind} {node_id}\n\n{self._marker(node_id, epoch)}"

    def _fail_text(self, base: str, diff_path: Path | None) -> str:
        if diff_path is None:
            return base
        return f"{base}\n[dirty working-tree diff captured: {diff_path}]"
