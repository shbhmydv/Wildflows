from __future__ import annotations
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import NoReturn
from wildflows.admission import admit_epoch
from wildflows.events import Boundary, Dispatched, Integrated, LoopIter, ResultEvent
from wildflows.expr import CtxRef, Dispatch, Do, Expr, Inplace, Loop, Seq, Until
from wildflows.journal import Journal
from wildflows.projection import ExecutionOutcome, Floor, NodeKey, RunProjection, replay
from wildflows.result import IntegrationReceipt, Result
from wildflows.rig import RigRegistry, run_shell
from wildflows.workspace import (
    BranchDivergedError,
    IntegrationError,
    NodeWorktree,
    Repository,
    RepositoryError,
    RepositoryTransientError,
)
__all__ = [
    "Engine",
    "NodeExecutionError",
    "SiblingOwnershipError",
    "PredicateEvaluationError",
    "ResumeVerificationError",
    "BranchDivergedError",
    "RepositoryTransientError",
    "replay",
    "RunProjection",
]
class NodeExecutionError(RuntimeError):
    """A node failed; its worktree was abandoned and the epoch remains open."""
class SiblingOwnershipError(NodeExecutionError):
    """A later concurrent sibling touched a path already owned by a landed sibling."""
class PredicateEvaluationError(RuntimeError):
    """A command predicate timed out or could not be evaluated."""
class ResumeVerificationError(RuntimeError):
    """Journalled Git claims cannot be reconciled with the run branch."""
@dataclass(frozen=True)
class _Attempt:
    node: Do | Inplace
    key: NodeKey
    base: str
    worktree: NodeWorktree
    prompt: str | None = None
@dataclass(frozen=True)
class _Candidate:
    result: Result
    commit: str
    receipt: IntegrationReceipt
class Engine:
    def __init__(
        self,
        run_dir: Path,
        workdir: Path,
        registry: RigRegistry,
        run_branch: str | None = None,
        max_workers: int = 1,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.repo = Repository(workdir, self.run_dir, run_branch)
        self.registry = registry
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self.max_workers = max_workers
        self.run_id = self.run_dir.name
        with self.repo.integration_guard():
            self.journal = Journal.load(self.run_dir)
            for epoch in self.journal.projection.epochs.values():
                if epoch.run_branch is not None and epoch.run_branch != self.repo.ref:
                    raise ResumeVerificationError(
                        f"journal belongs to {epoch.run_branch!r}, not {self.repo.ref!r}"
                    )
            self._verify_resume_claims()
    @property
    def workdir(self) -> Path:
        return self.repo.root
    @property
    def run_branch(self) -> str:
        return self.repo.branch
    @property
    def _proj(self) -> RunProjection:
        return self.journal.projection
    def run_epoch(self, tree: Expr, epoch: int) -> None:
        with self.repo.integration_guard():
            fresh = Journal.load(self.run_dir)
            if fresh.events() != self.journal.events():
                self.journal = fresh
            self._run_epoch(tree, epoch)
    def _run_epoch(self, tree: Expr, epoch: int) -> None:
        self._verify_resume_claims()
        tree = admit_epoch(tree, epoch, self._proj, self.registry)
        if self._proj.epoch_closed(epoch):
            return
        if not self._proj.epoch_opened(epoch):
            base = self.repo.branch_tip()
            self.journal.append(
                Boundary(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=tree.node_id,
                    phase="opened",
                    expr=tree.model_dump(),
                    run_branch=self.repo.ref,
                    base_commit=base,
                )
            )
        self._exec(tree, epoch)
        self._verify_resume_claims()
        self.journal.append(
            Boundary(
                run_id=self.run_id,
                epoch=epoch,
                node_id=tree.node_id,
                phase="closed",
                reason="done",
            )
        )
    def _verify_resume_claims(self) -> None:
        while self._verify_once():
            pass
    def _verify_once(self) -> bool:
        events = self._proj.effective_events
        if not events:
            return False
        dispatches: dict[NodeKey, Dispatched] = {}
        results: dict[NodeKey, ResultEvent] = {}
        integrated_after: dict[NodeKey, int] = {}
        prefix_dependents: dict[str, tuple[int, int]] = {}
        verified_tips: set[str] = set()
        expected: str | None = None
        for event in events:
            if isinstance(event, Boundary) and event.phase == "opened":
                if event.run_branch is None or event.base_commit is None:
                    raise ResumeVerificationError(
                        "open epoch predates the worktree model and cannot be resumed"
                    )
                if event.run_branch != self.repo.ref:
                    raise ResumeVerificationError(
                        f"epoch names run branch {event.run_branch!r}, not {self.repo.ref!r}"
                    )
                if expected is None:
                    expected = event.base_commit
                    verified_tips.add(expected)
                elif event.base_commit != expected:
                    raise ResumeVerificationError(
                        "epoch base does not match the preceding verified journal tip"
                    )
                continue
            if isinstance(event, Dispatched):
                dispatches[(event.epoch, event.node_id)] = event
                if expected is not None:
                    prefix_dependents.setdefault(expected, (event.epoch, event.seq))
                continue
            if isinstance(event, ResultEvent):
                key = (event.epoch, event.node_id)
                results[key] = event
                dispatch = dispatches.get(key)
                if (
                    event.integration_base is not None
                    and dispatch is not None
                    and event.integration_base != dispatch.pre_head
                ):
                    prefix_dependents.setdefault(
                        event.integration_base, (event.epoch, event.seq)
                    )
                continue
            if not isinstance(event, Integrated):
                continue
            key = (event.epoch, event.node_id)
            dispatch = dispatches.get(key)
            result = results.get(key)
            base = (
                result.integration_base if result is not None else None
            ) or (dispatch.pre_head if dispatch is not None else None)
            if (
                expected is None or dispatch is None or base is None or result is None
                or not (dispatch.seq < result.seq < event.seq)
            ):
                raise ResumeVerificationError(
                    f"integrated claim at seq {event.seq} has no contiguous attempt provenance"
                )
            if base != expected:
                raise ResumeVerificationError(
                    f"integrated claim at seq {event.seq} does not follow the verified tip"
                )
            if (
                not result.ok or not result.receipt_required
                or result.post_head != event.commit
            ):
                if self._fallback_failed_claim(
                    key[0], self._claim_start(dispatch, result),
                    f"resume fallback: integrated seq {event.seq} contradicts its result",
                    expected, event.commit, prefix_dependents,
                ):
                    return True
                raise ResumeVerificationError(
                    f"integrated claim at seq {event.seq} contradicts its result certificate"
                )
            try:
                self.repo.verify_receipt(base, event.commits)
            except RepositoryError as exc:
                if self._fallback_failed_claim(
                    key[0], self._claim_start(dispatch, result),
                    f"resume fallback: unverifiable receipt at seq {event.seq}: {exc}",
                    expected, event.commit, prefix_dependents,
                ):
                    return True
                raise ResumeVerificationError(
                    f"receipt at seq {event.seq} is unverifiable and its effect is live: {exc}"
                ) from exc
            expected = event.commit
            verified_tips.add(expected)
            integrated_after[key] = event.seq
        if expected is None:
            raise ResumeVerificationError("journal has no worktree-model opened boundary")
        live = self.repo.branch_claim()
        outstanding = [
            (key, result)
            for key, result in results.items()
            if result.ok
            and result.receipt_required
            and integrated_after.get(key, -1) < result.seq
        ]
        if len(outstanding) > 1:
            raise ResumeVerificationError("journal has multiple unintegrated successful attempts")
        if outstanding:
            key, result = outstanding[0]
            dispatch = dispatches.get(key)
            base = result.integration_base or (
                dispatch.pre_head if dispatch is not None else None
            )
            candidate = result.post_head
            if base != expected or candidate is None:
                if live == expected:
                    assert dispatch is not None
                    self._journal_fallback(
                        key[0], dispatch.seq, "resume fallback: incomplete integration claim",
                        expected,
                    )
                    return True
                raise BranchDivergedError("run branch does not match an incomplete claim")
            if live == candidate:
                if not self.repo.commit_exists(candidate):
                    assert dispatch is not None
                    if self._fallback_failed_claim(
                        key[0], self._claim_start(dispatch, result),
                        "resume fallback: incomplete integration candidate is missing",
                        expected, candidate, prefix_dependents,
                    ):
                        return True
                try:
                    receipt = self.repo.receipt(base, candidate)
                except RepositoryError as exc:
                    raise ResumeVerificationError(
                        f"could not reconstruct landed integration: {exc}"
                    ) from exc
                if not receipt.commits:
                    raise ResumeVerificationError("effectful result reconstructed no commits")
                self._append_integrated(key, receipt)
                return True
            if live == expected:
                assert dispatch is not None
                cleaned = self.repo.restore_interrupted_integration(expected, candidate)
                reason = "resume fallback: result was journalled but its commits did not land"
                if cleaned:
                    reason += "; removed ownerless interrupted Git lock residue"
                self._journal_fallback(
                    key[0], self._claim_start(dispatch, result), reason, expected
                )
                return True
            if self._fallback_known_prefix(prefix_dependents, live):
                return True
            raise BranchDivergedError(
                f"run branch moved outside incomplete attempt: expected {expected} "
                f"or {candidate}, found {live}"
            )
        if not self.repo.commit_exists(live):
            raise BranchDivergedError(
                f"run branch {self.repo.branch!r} has unknown or missing tip {live}"
            )
        if live != expected:
            if self._fallback_known_prefix(prefix_dependents, live):
                return True
            raise BranchDivergedError(
                f"run branch {self.repo.branch!r} has operator commits: "
                f"journal expects {expected}, found {live}"
            )
        unfinished = [
            (key, node)
            for key, node in self._proj.nodes.items()
            if node.last_dispatch_seq > node.result_seq
        ]
        if unfinished:
            key, node = min(unfinished, key=lambda item: item[1].last_dispatch_seq)
            if node.dispatched_pre_head == expected:
                self._journal_fallback(
                    key[0], node.last_dispatch_seq,
                    "resume fallback: interrupted attempt abandoned", expected,
                )
                return True
            if node.dispatched_pre_head in verified_tips:
                self._append_result(
                    key,
                    Result(
                        text="resume: interrupted concurrent sibling abandoned",
                        outcome="failed",
                    ),
                    post_head=expected,
                )
                return True
            raise ResumeVerificationError("unfinished attempt did not start at verified tip")
        return False
    @staticmethod
    def _claim_start(dispatch: Dispatched, result: ResultEvent) -> int:
        if (
            result.integration_base is not None
            and result.integration_base != dispatch.pre_head
        ):
            return result.seq
        return dispatch.seq
    def _fallback_failed_claim(
        self,
        epoch_id: int,
        start: int,
        text: str,
        expected: str,
        claimed: str,
        prefix_dependents: dict[str, tuple[int, int]],
    ) -> bool:
        live = self.repo.branch_claim()
        if live == expected:
            reason = text
        elif live == claimed and not self.repo.commit_exists(claimed):
            cleaned = self.repo.restore_missing_claim(claimed, expected)
            reason = f"resume fallback: missing current claimed commit {claimed}"
            if cleaned:
                reason += "; removed ownerless interrupted Git lock residue"
        else:
            return self._fallback_known_prefix(prefix_dependents, live)
        self._journal_fallback(epoch_id, start, reason, expected)
        return True
    def _fallback_known_prefix(
        self, prefix_dependents: dict[str, tuple[int, int]], live: str
    ) -> bool:
        dependent = prefix_dependents.get(live)
        if dependent is None or not self.repo.commit_exists(live):
            return False
        epoch_id, start = dependent
        self._journal_fallback(
            epoch_id, start, "resume fallback: exact verified prefix rewind", live
        )
        return True
    def _journal_fallback(self, epoch_id: int, start: int, text: str, tip: str) -> None:
        self.repo.sync_run_ref()
        epoch = self._proj.epochs[epoch_id]
        self.journal.append(Boundary(
            run_id=self.run_id, epoch=epoch_id, node_id="n0", phase="opened",
            expr=epoch.expr, run_branch=self.repo.ref, base_commit=tip,
            reason=text, fallback_from=start,
        ))
    def _exec(self, node: Expr, epoch: int, floor: Floor = -1) -> ExecutionOutcome:
        key = (epoch, node.node_id)
        if isinstance(node, (Do, Inplace)) and self._proj.resume_action(key, floor) == "done":
            return ExecutionOutcome(key=key)
        if isinstance(node, Dispatch):
            if self.max_workers > 1 and all(
                isinstance(child, (Do, Inplace)) for child in node.children
            ):
                return self._exec_dispatch(node, epoch, floor)
            return self._exec_children(node.children, epoch, floor)
        if isinstance(node, Seq):
            return self._exec_children(node.children, epoch, floor)
        if isinstance(node, Loop):
            return self._exec_loop(node, epoch, floor)
        if isinstance(node, Do):
            return self._exec_do(node, epoch)
        if isinstance(node, Inplace):
            return self._exec_inplace(node, epoch)
        return ExecutionOutcome(key=key)
    def _exec_children(
        self, children: list[Expr], epoch: int, floor: Floor
    ) -> ExecutionOutcome:
        outcomes: list[ExecutionOutcome] = []
        failures: list[RuntimeError] = []
        for child in children:
            try:
                outcomes.append(self._exec(child, epoch, floor))
            except (NodeExecutionError, PredicateEvaluationError) as exc:
                outcomes.append(ExecutionOutcome(key=(epoch, child.node_id)))
                failures.append(exc)
        if failures:
            raise NodeExecutionError("; ".join(str(exc) for exc in failures))
        return ExecutionOutcome(children=tuple(outcomes))
    def _prepare_attempt(self, node: Do | Inplace, epoch: int, base: str) -> _Attempt:
        key = (epoch, node.node_id)
        attempt = self._proj.node(key).dispatch_count
        worktree = self.repo.create_worktree(epoch, node.node_id, attempt, base)
        if isinstance(node, Do):
            self.journal.append(Dispatched(
                run_id=self.run_id, epoch=epoch, node_id=node.node_id,
                rig=node.rig.name, task=node.task, workdir=str(worktree.path),
                pre_head=base,
            ))
            prompt = self._materialize_ctx(node, epoch, worktree)
            if prompt is None:
                self.repo.remove_worktree(worktree)
                self._fail_node(key, base, "unresolved or escaping context reference")
            return _Attempt(node, key, base, worktree, prompt)
        self.journal.append(Dispatched(
            run_id=self.run_id, epoch=epoch, node_id=node.node_id,
            task=f"inplace: {len(node.edits)} edit(s)",
            workdir=str(worktree.path), pre_head=base,
        ))
        return _Attempt(node, key, base, worktree)
    def _build_candidate(self, attempt: _Attempt) -> _Candidate:
        node = attempt.node
        if isinstance(node, Do):
            assert attempt.prompt is not None
            try:
                result = self.registry.resolve(node.rig.name).run(
                    attempt.prompt, attempt.worktree.path
                )
            except Exception as exc:
                result = Result(text=f"rig raised: {exc}", outcome="failed")
            if not result.ok:
                return _Candidate(result, attempt.base, IntegrationReceipt())
            try:
                self.repo.commit_all(
                    attempt.worktree.path, self._commit_message("do", attempt.key)
                )
                candidate = self.repo.head(attempt.worktree.path)
                receipt = self.repo.receipt(attempt.base, candidate)
            except RepositoryError as exc:
                return _Candidate(
                    Result(text=f"do integration failed: {exc}", outcome="failed"),
                    attempt.base,
                    IntegrationReceipt(),
                )
            return _Candidate(result, candidate, receipt)
        paths = [edit.path for edit in node.edits]
        try:
            for edit in node.edits:
                target = self.repo.safe_path(
                    attempt.worktree.path, edit.path, for_write=True
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(edit.content, encoding="utf-8")
            self.repo.commit_declared(
                attempt.worktree.path,
                paths,
                self._commit_message("inplace", attempt.key),
            )
            candidate = self.repo.head(attempt.worktree.path)
            receipt = self.repo.receipt(attempt.base, candidate)
            undeclared = [path for path in receipt.paths if path not in set(paths)]
            if undeclared:
                raise IntegrationError(
                    f"inplace commit contains undeclared paths: {', '.join(undeclared)}"
                )
        except (OSError, UnicodeError, ValueError, RepositoryError) as exc:
            return _Candidate(
                Result(text=f"inplace failed: {exc}", outcome="failed"),
                attempt.base,
                IntegrationReceipt(),
            )
        result = Result(
            text=(
                f"wrote {', '.join(paths)}"
                if receipt.commits else "inplace: no diff (already applied)"
            ),
            files=paths if receipt.commits else [],
        )
        return _Candidate(result, candidate, receipt)
    def _exec_do(self, node: Do, epoch: int) -> ExecutionOutcome:
        return self._exec_leaf(node, epoch)
    def _exec_inplace(self, node: Inplace, epoch: int) -> ExecutionOutcome:
        return self._exec_leaf(node, epoch)
    def _exec_leaf(self, node: Do | Inplace, epoch: int) -> ExecutionOutcome:
        self._verify_resume_claims()
        attempt = self._prepare_attempt(node, epoch, self.repo.branch_tip())
        try:
            candidate = self._build_candidate(attempt)
            if not candidate.result.ok:
                self._record_failed_result(attempt.key, attempt.base, candidate.result)
            self._complete_attempt(
                attempt.key, attempt.base, candidate.commit,
                candidate.result, candidate.receipt,
            )
            return ExecutionOutcome(key=attempt.key)
        finally:
            self.repo.remove_worktree(attempt.worktree)
    def _exec_dispatch(
        self, node: Dispatch, epoch: int, floor: Floor
    ) -> ExecutionOutcome:
        """Run eligible leaf siblings together; this thread alone lands and journals."""
        self._verify_resume_claims()
        base = self.repo.branch_tip()
        outcomes = [
            ExecutionOutcome(key=(epoch, child.node_id)) for child in node.children
        ]
        attempts: list[_Attempt] = []
        failures: list[NodeExecutionError] = []
        owned_paths: set[str] = set()
        try:
            for child in node.children:
                assert isinstance(child, (Do, Inplace))
                key = (epoch, child.node_id)
                if self._proj.resume_action(key, floor) == "done":
                    continue
                try:
                    attempts.append(self._prepare_attempt(child, epoch, base))
                except NodeExecutionError as exc:
                    failures.append(exc)
            with ThreadPoolExecutor(
                max_workers=min(self.max_workers, len(attempts) or 1)
            ) as executor:
                pending: dict[Future[_Candidate], _Attempt] = {
                    executor.submit(self._build_candidate, attempt): attempt
                    for attempt in attempts
                }
                for future in as_completed(pending):
                    attempt = pending[future]
                    candidate = future.result()
                    failure = self._land_sibling(
                        attempt, candidate, owned_paths, epoch
                    )
                    if failure is not None:
                        failures.append(failure)
        finally:
            for attempt in attempts:
                self.repo.remove_worktree(attempt.worktree)
        if len(failures) == 1:
            raise failures[0]
        if failures:
            raise NodeExecutionError("; ".join(str(exc) for exc in failures))
        return ExecutionOutcome(children=tuple(outcomes))
    def _land_sibling(
        self,
        attempt: _Attempt,
        candidate: _Candidate,
        owned_paths: set[str],
        epoch: int,
    ) -> NodeExecutionError | None:
        current = self.repo.branch_tip()
        if not candidate.result.ok:
            self._append_result(attempt.key, candidate.result, post_head=current)
            return NodeExecutionError(
                candidate.result.text or f"node {attempt.key[1]} failed"
            )
        if not candidate.receipt.commits:
            self._append_result(attempt.key, candidate.result, post_head=current)
            return None
        try:
            self.repo.verify_receipt(attempt.base, candidate.receipt.commits)
        except RepositoryError as exc:
            text = f"sibling candidate receipt is invalid: {exc}"
            self._append_result(
                attempt.key, Result(text=text, outcome="failed"), post_head=current
            )
            return NodeExecutionError(text)
        overlap = owned_paths.intersection(candidate.receipt.paths)
        if overlap:
            text = "concurrent sibling ownership overlaps: " + ", ".join(sorted(overlap))
            self._append_result(
                attempt.key, Result(text=text, outcome="failed"), post_head=current
            )
            return SiblingOwnershipError(text)
        landing_commit = candidate.commit
        landing_receipt = candidate.receipt
        if current != attempt.base:
            combine = self.repo.create_worktree(
                epoch,
                f"{attempt.key[1]}.integrate",
                self._proj.node(attempt.key).dispatch_count,
                current,
            )
            try:
                self.repo.cherry_pick(combine.path, candidate.receipt.commits)
                landing_commit = self.repo.head(combine.path)
                landing_receipt = self.repo.receipt(current, landing_commit)
                source_paths = [item.paths for item in candidate.receipt.commits]
                if [item.paths for item in landing_receipt.commits] != source_paths:
                    raise IntegrationError("reapplied sibling changed its per-commit paths")
            except RepositoryError as exc:
                text = f"sibling reapply failed: {exc}"
                self._append_result(
                    attempt.key, Result(text=text, outcome="failed"), post_head=current
                )
                return NodeExecutionError(text)
            finally:
                self.repo.remove_worktree(combine)
        try:
            self._complete_attempt(
                attempt.key, current, landing_commit,
                candidate.result, landing_receipt,
            )
        except NodeExecutionError as exc:
            return exc
        owned_paths.update(candidate.receipt.paths)
        return None
    def _complete_attempt(
        self,
        key: NodeKey,
        base: str,
        candidate: str,
        result: Result,
        receipt: IntegrationReceipt,
    ) -> None:
        self.repo.ensure_tip(base)
        self._append_result(
            key,
            result,
            post_head=candidate,
            receipt_required=bool(receipt.commits),
            integration_base=base,
        )
        if not receipt.commits:
            return
        try:
            self.repo.integrate(base, candidate)
        except IntegrationError as exc:
            self._append_result(
                key,
                Result(text=f"run-branch integration failed: {exc}", outcome="failed"),
                post_head=base,
            )
            raise NodeExecutionError(str(exc)) from exc
        self._append_integrated(key, receipt)
    def _exec_loop(self, node: Loop, epoch: int, floor: Floor) -> ExecutionOutcome:
        key = (epoch, node.node_id)
        state = self._proj.node(key)
        if (
            floor is not None and state.result is not None and state.result_seq > floor
            and state.loop_status is not None
        ):
            return ExecutionOutcome(key=key)
        resume_from, partial_floor, last_converged, last_body = self._proj.loop_resume(
            key, floor
        )
        if resume_from and (last_converged or resume_from >= node.cap):
            self._finish_loop(node, epoch, last_body, resume_from, last_converged)
            return ExecutionOutcome(key=key)
        iterations = resume_from
        converged = False
        for index in range(resume_from, node.cap):
            body_floor: Floor = partial_floor if index == resume_from else None
            outcome = self._exec(node.body, epoch, body_floor)
            body_key = outcome.result_key()
            assert body_key is not None
            body = self._proj.result(body_key)
            if body is not None:
                last_body = body
            body_seq = self._proj.node(body_key).result_seq
            converged = self._exec_predicate(node, epoch)
            self.journal.append(
                LoopIter(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=node.node_id,
                    iteration=index,
                    commit=self.repo.branch_tip(),
                    converged=converged,
                    body_result_seq=body_seq,
                )
            )
            iterations = index + 1
            partial_floor = None
            if converged:
                break
        self._finish_loop(node, epoch, last_body, iterations, converged)
        return ExecutionOutcome(key=key)
    def _finish_loop(
        self,
        node: Loop,
        epoch: int,
        body: Result | None,
        iterations: int,
        converged: bool,
    ) -> None:
        status = (
            f"converged after {iterations} iteration(s)"
            if converged
            else f"hit cap {node.cap} without convergence (partial progress preserved)"
        )
        result = Result(
            text=body.text if body is not None else "",
            files=body.files if body is not None else [],
            exit_code=body.exit_code if body is not None else None,
            outcome="ok" if converged else "failed",
        )
        self._append_result(
            (epoch, node.node_id),
            result,
            post_head=self.repo.branch_tip(),
            loop_status=status,
        )
    def _until_command(self, until: Until) -> str:
        assert until.cmd is not None
        return until.cmd
    def _exec_predicate(self, node: Loop, epoch: int) -> bool:
        key = (epoch, f"{node.node_id}.until")
        self._verify_resume_claims()
        base = self.repo.branch_tip()
        attempt = self._proj.node(key).dispatch_count
        worktree = self.repo.create_worktree(epoch, key[1], attempt, base)
        command = self._until_command(node.until)
        try:
            self.journal.append(
                Dispatched(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=key[1],
                    cmd=command,
                    workdir=str(worktree.path),
                    pre_head=base,
                )
            )
            try:
                evaluation = run_shell(command, worktree.path, node.until.timeout_s)
            except OSError as exc:
                self._append_result(
                    key,
                    Result(text=f"predicate launch failed: {exc}", outcome="failed"),
                    post_head=base,
                )
                raise PredicateEvaluationError(str(exc)) from exc
            self.repo.ensure_tip(base)
            if evaluation.timed_out:
                text = f"predicate timed out after {node.until.timeout_s}s"
                self._append_result(
                    key, Result(text=text, outcome="failed"), post_head=base
                )
                raise PredicateEvaluationError(text)
            assert evaluation.returncode is not None
            text = evaluation.stdout if evaluation.returncode == 0 else (
                evaluation.stderr or evaluation.stdout
            )
            self._append_result(
                key,
                Result(text=text, exit_code=evaluation.returncode),
                post_head=base,
            )
            return evaluation.returncode == 0
        finally:
            self.repo.remove_worktree(worktree)
    def _append_result(
        self,
        key: NodeKey,
        result: Result,
        *,
        post_head: str | None,
        receipt_required: bool = False,
        integration_base: str | None = None,
        loop_status: str | None = None,
    ) -> None:
        self._persist_artifact(key, result)
        self.journal.append(
            ResultEvent(
                run_id=self.run_id,
                epoch=key[0],
                node_id=key[1],
                text=result.text,
                files=result.files,
                exit_code=result.exit_code,
                outcome=result.outcome,
                post_head=post_head,
                integration_base=integration_base,
                receipt_required=receipt_required,
                loop_status=loop_status,
            )
        )
    def _append_integrated(self, key: NodeKey, receipt: IntegrationReceipt) -> None:
        self.journal.append(
            Integrated(
                run_id=self.run_id,
                epoch=key[0],
                node_id=key[1],
                commits=receipt.commits,
            )
        )
    def _persist_artifact(self, key: NodeKey, result: Result) -> None:
        node = key[1].replace("/", "-").replace("..", "-")
        directory = self.run_dir / "artifacts" / f"e{key[0]}-{node}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"result-{self.journal.n_events}.json"
        temporary = path.with_suffix(".tmp")
        payload = json.dumps(
            result.model_dump(mode="json", exclude_computed_fields=True), sort_keys=True
        ).encode("utf-8")
        with open(temporary, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    def _record_failed_result(self, key: NodeKey, base: str, result: Result) -> NoReturn:
        self.repo.ensure_tip(base)
        self._append_result(key, result, post_head=base)
        raise NodeExecutionError(result.text or f"node {key[1]} failed")
    def _fail_node(self, key: NodeKey, base: str, text: str) -> NoReturn:
        self._record_failed_result(key, base, Result(text=text, outcome="failed"))
    def _materialize_ctx(self, node: Do, epoch: int, worktree: NodeWorktree) -> str | None:
        if not node.ctx:
            return node.task
        parts = [node.task]
        for ref in node.ctx:
            block = self._resolve_ctx(ref, epoch, worktree.path)
            if block is None:
                return None
            parts.append(block)
        return "\n\n".join(parts)
    def _resolve_ctx(self, ref: CtxRef, epoch: int, worktree: Path) -> str | None:
        if ref.kind == "file":
            content = self.repo.read_file(worktree, ref.ref)
            if content is None:
                return None
            return f"## Context: file {ref.ref}\n{content}"
        text = self._proj.result_text((epoch, ref.ref))
        if text is None:
            return None
        return f"## Context: node {ref.ref}\n{text}"
    def _commit_message(self, kind: str, key: NodeKey) -> str:
        return f"wildflows {kind} {key[1]} (run {self.run_id}, epoch {key[0]})"
