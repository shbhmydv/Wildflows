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

import base64
import os
from pathlib import Path
from typing import Literal, NoReturn

from wildflows.admission import admit_epoch
from wildflows.events import Boundary, Dispatched, LoopIter
from wildflows.expr import CtxRef, Dispatch, Do, Expr, Inplace, Loop, Seq, Until
from wildflows.journal import Journal
from wildflows.projection import ExecutionOutcome, Floor, NodeKey, RunProjection, replay
from wildflows.result import CommitReceipt, IntegrationReceipt, Result
from wildflows.rig import RigRegistry
from wildflows.workspace import (
    CompletionRecorder,
    InplaceIntent,
    IntentWrite,
    Lease,
    WorkspaceEffects,
    WorkspaceFault,
)

__all__ = ["Engine", "replay", "RunProjection"]


class Engine:
    def __init__(self, run_dir: Path, workdir: Path, registry: RigRegistry) -> None:
        self.run_dir = Path(run_dir)
        resolved_workdir = Path(workdir).resolve()
        if self.run_dir.resolve().is_relative_to(resolved_workdir):
            raise ValueError(
                "run_dir must be outside workdir in the shared-workdir engine; "
                "an unsandboxed rig could otherwise mutate the journal"
            )
        self.registry = registry
        # Load-continues-seq: a restart reuses the durable journal; its live projection is
        # folded from disk on load and updated on every append. Serial restarts only —
        # parallel dispatch (step 3) needs the single append owner this journal is (§6).
        self.journal = Journal.load(self.run_dir)
        self.run_id = self.run_dir.name
        self.ws = WorkspaceEffects(resolved_workdir, self.run_dir)
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
        if isinstance(node, (Do, Inplace)):
            action = self._proj.resume_action(key, floor)
            if action == "done":
                return ExecutionOutcome(key=key)
            if action == "recover" and not self._recover_unclean(key):
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
        node = self._proj.node(key)
        if floor is None or node.result_seq <= floor:
            return False
        if node.result is None or not node.result.ok:
            return False  # dispatched-only or failed: no completion certificate to recover
        try:
            receipt = self.ws.reconstruct_receipt(
                node.dispatched_pre_head, node.result_post_head
            )
            active = self.ws.certificate_is_active(
                node.result_post_head, receipt.paths
            )
        except WorkspaceFault as fault:
            self._halt_unclean(key, "result certificate validation", fault, "retry")
        if not receipt.commits or not active:
            self._recover_inactive_certificate(key, node.result_post_head)
            return False
        self.rec.record_integrated(key, receipt)
        return True

    def _recover_inactive_certificate(self, key: NodeKey, post_head: str | None) -> None:
        if post_head is None:
            raise WorkspaceFault("effectful result has no post_head certificate")
        node = self._proj.node(key)
        epoch, node_id = key
        attempt = node.dispatch_count - 1
        try:
            record = self.ws.load_lease_record(epoch, node_id, attempt)
            if record is not None and record.pre_head != node.dispatched_pre_head:
                raise WorkspaceFault("lease record pre_head contradicts dispatched provenance")
            self.ws.quarantine_inactive_certificate(
                epoch, node_id, attempt, node.dispatched_pre_head, post_head, record
            )
        except WorkspaceFault as fault:
            self._halt_unclean(key, "inactive result certificate recovery", fault, "retry")
        self.rec.record_result(
            key,
            Result(text="inactive result certificate quarantined; retry required", outcome="failed"),
            post_head=self.ws.head_commit(),
            recovery_action="retry",
        )
        self.ws.settle_records(epoch, node_id, attempt)

    def _recover_unclean(self, key: NodeKey) -> bool:
        """Retry checked cleanup for a durably halted node before any new mutation.

        Returns whether the interrupted attempt lacked a completion certificate and must
        dispatch again. Both lease and intent records are loaded (and therefore validated)
        before rollback/reset starts, so a corrupt present record never takes the legacy
        fallback and never permits a partial cleanup. The explicit clean result is the
        durable halt-clear transition; ``recovery_action='retry'`` keeps it non-terminal
        across a crash before redispatch.
        """
        node = self._proj.node(key)
        action = node.recovery_action
        if action is None:
            raise WorkspaceFault(
                "workspace is durably unclean but its legacy result has no recovery action; "
                "manual repair is required"
            )
        epoch, node_id = key
        attempt = node.dispatch_count - 1
        try:
            lease_record = self.ws.load_lease_record(epoch, node_id, attempt)
            intent = self.ws.load_intent(epoch, node_id, attempt)
            if (
                lease_record is not None
                and lease_record.pre_head != node.dispatched_pre_head
            ):
                raise WorkspaceFault("lease record pre_head contradicts dispatched provenance")
            if intent is not None:
                self.ws.rollback_inplace(
                    intent,
                    None if lease_record is None or lease_record.preexisting_dirs is None
                    else set(lease_record.preexisting_dirs),
                )
            if lease_record is not None:
                self.ws.quarantine_dead_attempt(lease_record)
            else:
                self.ws.quarantine_from_journal(
                    epoch, node_id, attempt, node.dispatched_pre_head)
        except WorkspaceFault as fault:
            self._halt_unclean(key, "workspace recovery", fault, action)
        self.rec.record_result(
            key,
            Result(text="workspace cleanup recovered", outcome="failed"),
            post_head=self.ws.head_commit(),
            recovery_action="retry" if action == "retry" else None,
        )
        self.ws.settle_records(epoch, node_id, attempt)
        return action == "retry"

    def _settle_cleared_retry(self, key: NodeKey, floor: Floor) -> None:
        """Finish old-record settlement after a crash following the durable halt clear."""
        node = self._proj.node(key)
        if (
            node.recovery_action == "retry"
            and not node.workspace_unclean
            and not node.has_unfinished_dispatch(floor)
            and node.dispatch_count > 0
        ):
            self.ws.settle_records(key[0], key[1], node.dispatch_count - 1)

    def _exec_do(self, node: Do, epoch: int, floor: Floor = -1) -> ExecutionOutcome:
        key = (epoch, node.node_id)
        self._settle_cleared_retry(key, floor)
        if self._recover_committed_attempt(key, floor):
            self._settle(key)
            return ExecutionOutcome(key=key)
        attempt = self._proj.node(key).dispatch_count
        lease = self._open_lease_or_fail(key, floor, attempt)
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
            self._settle(key)
            return ExecutionOutcome(key=key)
        diff_name = f"e{epoch}-{node.node_id}.diff"
        try:
            result = self.registry.resolve(node.rig.name).run(prompt, self.workdir)
        except Exception as exc:  # a rig exception never escapes after `dispatched`
            diff_path = self._finalize_failure(lease, diff_name, key)
            self.rec.record_result(key, Result(
                text=self._fail_text(f"rig raised: {exc}", diff_path), outcome="failed"),
                post_head=self.ws.head_commit())
            self._settle(key)
            return ExecutionOutcome(key=key)
        if not result.ok:
            # The failed rig's effects (incl its own commits) are reverted + captured.
            diff_path = self._finalize_failure(lease, diff_name, key)
            self.rec.record_result(key, Result(
                text=self._fail_text(result.text, diff_path),
                files=result.files, exit_code=result.exit_code, outcome=result.outcome),
                post_head=self.ws.head_commit())
            self._settle(key)
            return ExecutionOutcome(key=key)
        integ = self.ws.finalize_do_success(lease, self._commit_msg("do", node.node_id, epoch))
        if not integ.ok:
            # A git failure integrating a SUCCESSFUL rig's dirty state is a failed
            # transaction, not a bare error: revert + capture the leak through the same
            # failure path so no later node inherits it (hand-8, FAILURE-TRANSACTION).
            diff_path = self._finalize_failure(lease, diff_name, key)
            self.rec.record_result(key, Result(
                text=self._fail_text(f"do integration failed:\n{integ.stderr}", diff_path),
                outcome="failed"), post_head=self.ws.head_commit())
            self._settle(key)
            return ExecutionOutcome(key=key)
        # Result.files is the artifact list (== the effect paths in the shared-workdir
        # PoC); the receipt is the ownership record replay accumulates. `post_head` is the
        # completion certificate the torn-window receipt reconstruction is bounded by.
        self.rec.record_success(key, Result(
            text=result.text, files=integ.receipt.paths,
            exit_code=result.exit_code, outcome=result.outcome), integ.receipt,
            post_head=self.ws.head_commit())
        self._settle(key)
        return ExecutionOutcome(key=key)

    def _open_lease_or_fail(self, key: NodeKey, floor: Floor, attempt: int) -> Lease | None:
        """Open a lease, first QUARANTINING (never destroying) a dead dispatched-only
        attempt on a top-level resume (hand-10, PRINCIPLE A). A refused lease (dirty
        tracked/index worktree) records a durable failed result and returns None — the
        honest serial-M1 rule."""
        node = self._proj.node(key)
        if node.has_unfinished_dispatch(floor):
            self._quarantine_dead_attempt(key, attempt)
        lease = self.ws.open_lease(key, attempt)
        if lease is None:
            self.rec.record_result(key, Result(
                text="workdir has uncommitted tracked changes; lease refused",
                outcome="failed"), post_head=self.ws.head_commit())
        return lease

    def _quarantine_dead_attempt(self, key: NodeKey, attempt: int) -> None:
        """Quarantine + reset a dead attempt from its DURABLE lease record (PRINCIPLE B),
        or, for a pre-hand-10 dispatched line with no record, a conservative journal-based
        fallback that never sweeps (treats all current untracked as preexisting) while
        still quarantining committed work and resetting to the journalled `pre_head`. A
        cleanup git-op failure HALTS the epoch (WorkspaceFault, PRINCIPLE A)."""
        epoch, node_id = key
        dead = attempt - 1
        try:
            rec = self.ws.load_lease_record(epoch, node_id, dead)
            if (
                rec is not None
                and rec.pre_head != self._proj.node(key).dispatched_pre_head
            ):
                raise WorkspaceFault("lease record pre_head contradicts dispatched provenance")
            if rec is not None:
                self.ws.quarantine_dead_attempt(rec)
            else:
                self.ws.quarantine_from_journal(
                    epoch, node_id, dead, self._proj.node(key).dispatched_pre_head)
        except WorkspaceFault as fault:
            self._halt_unclean(key, "dead-attempt quarantine", fault, "retry")
        self.ws.settle_records(epoch, node_id, dead)

    def _finalize_failure(self, lease: Lease, diff_name: str, key: NodeKey) -> Path | None:
        """Run the checked failure revert; a cleanup git-op failure HALTS the epoch with a
        `workspace_unclean` failed result rather than a durable "failed" that lies the live
        effect was handled (hand-10, PRINCIPLE A)."""
        try:
            return self.ws.finalize_failure(lease, diff_name)
        except WorkspaceFault as fault:
            self._halt_unclean(key, "failure cleanup", fault, "fail")

    def _halt_unclean(
        self, key: NodeKey, stage: str, fault: WorkspaceFault,
        recovery_action: Literal["fail", "retry"],
    ) -> NoReturn:
        """Record the failed result marked `workspace_unclean` (honest: a live effect may
        survive), then re-raise to HALT the epoch — the fault is durable in the journal and
        no `boundary(closed)` is written for a workspace we could not clean."""
        self.rec.record_result(key, Result(
            text=self._fail_text(f"{stage} failed, workspace UNCLEAN: {fault}", fault.diff_path),
            outcome="failed"), post_head=self.ws.head_commit(), workspace_unclean=True,
            recovery_action=recovery_action)
        raise fault

    def _settle(self, key: NodeKey) -> None:
        """Remove the attempt's durable lease/intent records once its terminal result is
        journalled (PRINCIPLE B: settle only after the transaction is durable)."""
        epoch, node_id = key
        self.ws.settle_records(epoch, node_id, self._proj.node(key).dispatch_count - 1)

    def _exec_inplace(self, node: Inplace, epoch: int, floor: Floor = -1) -> ExecutionOutcome:
        key = (epoch, node.node_id)
        self._settle_cleared_retry(key, floor)
        if self._recover_committed_attempt(key, floor):
            self._settle(key)
            return ExecutionOutcome(key=key)
        attempt = self._proj.node(key).dispatch_count
        pnode = self._proj.node(key)
        if pnode.has_unfinished_dispatch(floor):
            # Reverse a crashed inplace's DURABLE intent BEFORE anything else (PRINCIPLE B):
            # a partial write to a pre-existing (possibly untracked) file is restored from
            # the fsynced originals, which a `reset --hard` alone could not recover.
            self._reverse_pending_intent(key, attempt - 1)
        lease = self._open_lease_or_fail(key, floor, attempt)
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
            self._settle(key)
            return ExecutionOutcome(key=key)
        # A durable intent (every target's pre-state) is fsynced BEFORE the first write, so
        # ANY exception after the first write — OR a crash — is reversed from the record,
        # leaving NO partial effect (hand-10, PRINCIPLE B / INPLACE-TRANSACTIONAL). A path
        # rejection at PLAN time (a symlink escape resolve_safe_path catches) is before any
        # mutation, so no rollback is needed.
        try:
            writes = self._plan_inplace_writes(node)
        except (ValueError, OSError) as exc:
            self.rec.record_result(key, Result(
                text=f"inplace path rejected: {exc}", outcome="failed"),
                post_head=self.ws.head_commit())
            self._settle(key)
            return ExecutionOutcome(key=key)
        intent = InplaceIntent(
            epoch=epoch, node_id=node.node_id, attempt=attempt, writes=writes,
            created_dirs=self._created_inplace_dirs(writes), ts=0.0)
        self.ws.write_intent(intent)
        paths = [w.path for w in writes]
        try:
            self._apply_inplace_writes(writes)
        except Exception as exc:  # OSError/UnicodeError/... — a partial write is rolled back
            self._rollback_inplace(intent, lease, key)
            self.rec.record_result(key, Result(
                text=f"inplace write failed: {exc}", outcome="failed"),
                post_head=self.ws.head_commit())
            self._settle(key)
            return ExecutionOutcome(key=key)
        integ = self.ws.integrate_declared(paths, self._commit_msg("inplace", node.node_id, epoch))
        if integ.status == "failed":
            self._rollback_inplace(
                intent, lease, key
            )  # no partial write survives a failed commit
            self.rec.record_result(key, Result(
                text=f"inplace integration failed:\n{integ.stderr}", outcome="failed"),
                post_head=self.ws.head_commit())
            self._settle(key)
            return ExecutionOutcome(key=key)
        if integ.status == "noop":
            # An already-identical edit produced no diff: a DURABLE no-op (files=[]), so
            # resume never re-applies it (DESIGN §5).
            self.rec.record_result(key, Result(
                text=f"inplace: no diff (already applied) for {', '.join(paths)}", files=[]),
                post_head=self.ws.head_commit())
            self._settle(key)
            return ExecutionOutcome(key=key)
        receipt = IntegrationReceipt(commits=[CommitReceipt(sha=integ.commit or "", paths=paths)])
        self.rec.record_success(key, Result(text=f"wrote {', '.join(paths)}", files=paths),
                                receipt, post_head=self.ws.head_commit())
        self._settle(key)
        return ExecutionOutcome(key=key)

    def _plan_inplace_writes(self, node: Inplace) -> list[IntentWrite]:
        """Plan one canonical resolved-target model for every transaction operation."""
        writes: list[IntentWrite] = []
        resolved_paths: set[str] = set()
        workdir = self.workdir.resolve()
        for edit in node.edits:
            resolved = self.ws.resolve_safe_path(edit.path)
            canonical = resolved.relative_to(workdir).as_posix()
            if canonical in resolved_paths:
                raise ValueError(f"resolved target collision: {edit.path!r} -> {canonical!r}")
            resolved_paths.add(canonical)
            if resolved.is_file():
                if resolved.stat().st_nlink != 1:
                    raise ValueError(
                        f"inplace target has hard-link aliases and cannot be canonicalized: "
                        f"{edit.path}"
                    )
                original_b64 = base64.b64encode(resolved.read_bytes()).decode("ascii")
                pre_kind: Literal["file", "dir", "absent"] = "file"
            elif resolved.is_dir():
                original_b64 = None
                pre_kind = "dir"  # write raises; rollback leaves the preexisting dir intact
            elif os.path.lexists(resolved):
                raise ValueError(f"inplace target is not a regular file: {edit.path}")
            else:
                original_b64 = None
                pre_kind = "absent"
            writes.append(IntentWrite(
                path=canonical, pre_kind=pre_kind, original_b64=original_b64,
                content=edit.content,
            ))
        return writes

    def _created_inplace_dirs(self, writes: list[IntentWrite]) -> list[str]:
        """Canonical parent directories that the write phase may create."""
        root = self.workdir.resolve()
        created: set[str] = set()
        for write in writes:
            parent = (root / write.path).parent
            while parent != root and not os.path.lexists(parent):
                created.add(parent.relative_to(root).as_posix())
                parent = parent.parent
        return sorted(created, key=lambda p: (len(Path(p).parts), p))

    def _apply_inplace_writes(self, writes: list[IntentWrite]) -> None:
        """Write only canonical targets recorded in the durable intent."""
        for write in writes:
            plain = self.workdir / write.path
            plain.parent.mkdir(parents=True, exist_ok=True)
            plain.write_text(write.content or "", encoding="utf-8")

    def _reverse_pending_intent(self, key: NodeKey, attempt: int) -> None:
        try:
            # Validate both durable records before the first reversal write. A corrupt
            # lease must halt without letting a valid intent partially mutate the tree.
            lease_record = self.ws.load_lease_record(key[0], key[1], attempt)
            if (
                lease_record is not None
                and lease_record.pre_head != self._proj.node(key).dispatched_pre_head
            ):
                raise WorkspaceFault("lease record pre_head contradicts dispatched provenance")
            intent = self.ws.load_intent(key[0], key[1], attempt)
            if intent is not None:
                self.ws.rollback_inplace(
                    intent,
                    None if lease_record is None or lease_record.preexisting_dirs is None
                    else set(lease_record.preexisting_dirs),
                )
        except WorkspaceFault as fault:
            self._halt_unclean(key, "pending inplace intent reversal", fault, "retry")

    def _rollback_inplace(self, intent: InplaceIntent, lease: Lease, key: NodeKey) -> None:
        """Roll back from the durable intent; an unstage git-op failure HALTS the epoch
        (PRINCIPLE A) rather than record a durable failure that leaves a staged partial."""
        try:
            self.ws.rollback_inplace(intent, set(lease.preexisting_dirs))
        except WorkspaceFault as fault:
            self._halt_unclean(key, "inplace rollback", fault, "fail")

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
