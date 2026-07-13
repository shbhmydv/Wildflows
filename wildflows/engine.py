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
import unicodedata
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
    RecoveryOutcome,
    RecoveryRequest,
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
            if self._publish_pending_recovery(key) == "fail":
                return ExecutionOutcome(key=key)
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

    def _recovery_request(
        self, key: NodeKey, action: Literal["fail", "retry"], result: Result,
        *, certified_post_head: str | None = None,
    ) -> RecoveryRequest:
        node = self._proj.node(key)
        return RecoveryRequest(
            node_key=key, attempt=node.dispatch_count - 1,
            expected_pre_head=node.dispatched_pre_head,
            lease_required=node.lease_required, action=action, result=result,
            evidence_kind="failed-diffs" if action == "fail" else "quarantine",
            certified_post_head=certified_post_head,
        )

    def _run_recovery(
        self, key: NodeKey, request: RecoveryRequest, stage: str
    ) -> RecoveryOutcome:
        try:
            return self.ws.recover_lease(request)
        except WorkspaceFault as fault:
            self._halt_unclean(key, stage, fault, request.action)

    def _publish_recovery(self, key: NodeKey, outcome: RecoveryOutcome) -> None:
        self.rec.record_result(
            key, outcome.result, post_head=self.ws.head_commit(),
            recovery_action="retry" if outcome.action == "retry" else None,
        )

    def _publish_pending_recovery(
        self, key: NodeKey
    ) -> Literal["fail", "retry"] | None:
        """Publish a recovery settled just before process death, before resume decisions."""
        node = self._proj.node(key)
        if node.dispatch_count == 0:
            return None
        if node.result_seq > node.last_dispatch_seq and not node.workspace_unclean:
            return None
        try:
            outcome = self.ws.resume_recovery(
                key, node.dispatch_count - 1, node.dispatched_pre_head
            )
        except WorkspaceFault as fault:
            self._halt_unclean(
                key, "settled recovery publication", fault, node.recovery_action or "retry"
            )
        if outcome is None:
            return None
        self._publish_recovery(key, outcome)
        return outcome.action

    def _recover_inactive_certificate(self, key: NodeKey, post_head: str | None) -> None:
        if post_head is None:
            raise WorkspaceFault("effectful result has no post_head certificate")
        outcome = self._run_recovery(
            key,
            self._recovery_request(
                key, "retry",
                Result(text="inactive result certificate quarantined; retry required",
                       outcome="failed"),
                certified_post_head=post_head,
            ),
            "inactive result certificate recovery",
        )
        self._publish_recovery(key, outcome)

    def _recover_unclean(self, key: NodeKey) -> bool:
        node = self._proj.node(key)
        action = node.recovery_action
        if action is None:
            raise WorkspaceFault(
                "workspace is durably unclean but its legacy result has no recovery action; "
                "manual repair is required"
            )
        outcome = self._run_recovery(
            key,
            self._recovery_request(
                key, action, Result(text="workspace cleanup recovered", outcome="failed")
            ),
            "workspace recovery",
        )
        self._publish_recovery(key, outcome)
        return action == "retry"

    def _exec_do(self, node: Do, epoch: int, floor: Floor = -1) -> ExecutionOutcome:
        key = (epoch, node.node_id)
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
            pre_head=lease.pre_head, lease_required=True,
        ))
        if prompt is None:  # an unresolvable/escaping ctx ref is a failed RESULT
            self.rec.record_result(key, Result(
                text=f"unresolved ctx for {node.node_id}", outcome="failed"),
                post_head=self.ws.head_commit())
            self._settle(key)
            return ExecutionOutcome(key=key)
        try:
            result = self.registry.resolve(node.rig.name).run(prompt, self.workdir)
        except Exception as exc:  # a rig exception never escapes after `dispatched`
            self._finish_live_failure(
                key, Result(text=f"rig raised: {exc}", outcome="failed")
            )
            return ExecutionOutcome(key=key)
        if not result.ok:
            self._finish_live_failure(key, result)
            return ExecutionOutcome(key=key)
        try:
            integ = self.ws.finalize_do_success(
                lease, self._commit_msg("do", node.node_id, epoch)
            )
        except WorkspaceFault as fault:
            self._finish_live_failure(
                key, Result(text=f"do integration failed:\n{fault}", outcome="failed")
            )
            return ExecutionOutcome(key=key)
        if not integ.ok:
            self._finish_live_failure(
                key, Result(text=f"do integration failed:\n{integ.stderr}", outcome="failed")
            )
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
            self._recover_dead_attempt(key)
        lease = self.ws.open_lease(key, attempt)
        if lease is None:
            self.rec.record_result(key, Result(
                text="workdir has uncommitted tracked changes; lease refused",
                outcome="failed"), post_head=self.ws.head_commit())
        return lease

    def _recover_dead_attempt(self, key: NodeKey) -> None:
        outcome = self._run_recovery(
            key,
            self._recovery_request(
                key, "retry", Result(text="dead attempt quarantined; retry required",
                                     outcome="failed")
            ),
            "dead-attempt recovery",
        )
        self._publish_recovery(key, outcome)

    def _finish_live_failure(self, key: NodeKey, result: Result) -> None:
        outcome = self._run_recovery(
            key, self._recovery_request(key, "fail", result), "failure cleanup"
        )
        self._publish_recovery(key, outcome)

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
        if self._recover_committed_attempt(key, floor):
            self._settle(key)
            return ExecutionOutcome(key=key)
        attempt = self._proj.node(key).dispatch_count
        lease = self._open_lease_or_fail(key, floor, attempt)
        if lease is None:
            return ExecutionOutcome(key=key)
        self.journal.append(Dispatched(
            run_id=self.run_id, epoch=epoch, node_id=node.node_id,
            task=f"inplace: {len(node.edits)} edit(s)", workdir=str(self.workdir),
            pre_head=lease.pre_head, lease_required=True,
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
            self._apply_inplace_writes(intent)
        except Exception as exc:  # OSError/UnicodeError/... — recover from durable intent
            self._finish_live_failure(
                key, Result(text=f"inplace write failed: {exc}", outcome="failed")
            )
            return ExecutionOutcome(key=key)
        try:
            integ = self.ws.integrate_declared(
                paths, self._commit_msg("inplace", node.node_id, epoch)
            )
        except WorkspaceFault as fault:
            self._finish_live_failure(
                key, Result(text=f"inplace integration failed:\n{fault}", outcome="failed")
            )
            return ExecutionOutcome(key=key)
        if integ.status == "failed":
            self._finish_live_failure(
                key, Result(text=f"inplace integration failed:\n{integ.stderr}", outcome="failed")
            )
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
        identities: dict[tuple[int, int], str] = {}
        case_keys: dict[str, str] = {}
        workdir = self.workdir.resolve()
        for edit in node.edits:
            resolved = self.ws.resolve_safe_path(edit.path)
            canonical_fs = resolved.relative_to(workdir).as_posix()
            if canonical_fs in resolved_paths:
                raise ValueError(
                    f"resolved target collision: {edit.path!r} -> {canonical_fs!r}"
                )
            resolved_paths.add(canonical_fs)
            case_key = unicodedata.normalize("NFC", canonical_fs).casefold()
            canonical = self.ws.encode_path(canonical_fs)
            if case_key in case_keys:
                raise ValueError(
                    f"portable case-canonical target collision: {case_keys[case_key]!r} "
                    f"and {edit.path!r}"
                )
            case_keys[case_key] = edit.path
            if os.path.lexists(resolved):
                info = resolved.stat()
                identity = (info.st_dev, info.st_ino)
                if identity in identities:
                    raise ValueError(
                        f"filesystem-identity target collision: {identities[identity]!r} "
                        f"and {edit.path!r}"
                    )
                identities[identity] = edit.path
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
            parent = (root / self.ws.decode_path(write.path)).parent
            while parent != root and not os.path.lexists(parent):
                created.add(self.ws.encode_path(parent.relative_to(root).as_posix()))
                parent = parent.parent
        return sorted(created, key=lambda p: (len(Path(self.ws.decode_path(p)).parts), p))

    def _apply_inplace_writes(self, intent: InplaceIntent) -> None:
        """Fsync per-path write-start, then write only canonical intent targets."""
        for write in intent.writes:
            write.started = True
            self.ws.write_intent(intent)
            plain = self.workdir / self.ws.decode_path(write.path)
            plain.parent.mkdir(parents=True, exist_ok=True)
            plain.write_text(write.content or "", encoding="utf-8")

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
