from __future__ import annotations
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
)
__all__ = [
    "Engine",
    "NodeExecutionError",
    "PredicateEvaluationError",
    "ResumeVerificationError",
    "BranchDivergedError",
    "replay",
    "RunProjection",
]
class NodeExecutionError(RuntimeError):
    """A node failed; its worktree was abandoned and the epoch remains open."""
class PredicateEvaluationError(RuntimeError):
    """A command predicate timed out or could not be evaluated."""
class ResumeVerificationError(RuntimeError):
    """Journalled Git claims cannot be reconciled with the run branch."""
class Engine:
    def __init__(
        self,
        run_dir: Path,
        workdir: Path,
        registry: RigRegistry,
        run_branch: str | None = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.repo = Repository(workdir, self.run_dir, run_branch)
        self.registry = registry
        self.journal = Journal.load(self.run_dir)
        self.run_id = self.run_dir.name
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
                elif event.base_commit != expected:
                    raise ResumeVerificationError(
                        "epoch base does not match the preceding verified journal tip"
                    )
                continue
            if isinstance(event, Dispatched):
                dispatches[(event.epoch, event.node_id)] = event
                continue
            if isinstance(event, ResultEvent):
                results[(event.epoch, event.node_id)] = event
                continue
            if not isinstance(event, Integrated):
                continue
            key = (event.epoch, event.node_id)
            dispatch = dispatches.get(key)
            result = results.get(key)
            base = dispatch.pre_head if dispatch is not None else None
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
            try:
                self.repo.verify_receipt(base, event.commits)
            except RepositoryError as exc:
                if self.repo.branch_tip() == expected:
                    self._journal_fallback(
                        key[0], dispatch.seq,
                        f"resume fallback: unverifiable receipt at seq {event.seq}: {exc}",
                        expected,
                    )
                    return True
                raise ResumeVerificationError(
                    f"receipt at seq {event.seq} is unverifiable and its effect is live: {exc}"
                ) from exc
            expected = event.commit
            integrated_after[key] = event.seq
        if expected is None:
            raise ResumeVerificationError("journal has no worktree-model opened boundary")
        live = self.repo.branch_tip()
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
            base = dispatch.pre_head if dispatch is not None else None
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
                self._journal_fallback(
                    key[0], dispatch.seq,
                    "resume fallback: result was journalled but its commits did not land",
                    expected,
                )
                return True
            raise BranchDivergedError(
                f"run branch moved outside incomplete attempt: expected {expected} "
                f"or {candidate}, found {live}"
            )
        if live != expected:
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
            if node.dispatched_pre_head != expected:
                raise ResumeVerificationError("unfinished attempt did not start at verified tip")
            self._journal_fallback(
                key[0], node.last_dispatch_seq,
                "resume fallback: interrupted attempt abandoned", expected,
            )
            return True
        return False
    def _journal_fallback(self, epoch_id: int, start: int, text: str, tip: str) -> None:
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
        if isinstance(node, (Seq, Dispatch)):
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
    def _exec_do(self, node: Do, epoch: int) -> ExecutionOutcome:
        key = (epoch, node.node_id)
        self._verify_resume_claims()
        base = self.repo.branch_tip()
        attempt = self._proj.node(key).dispatch_count
        worktree = self.repo.create_worktree(epoch, node.node_id, attempt, base)
        try:
            self.journal.append(
                Dispatched(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=node.node_id,
                    rig=node.rig.name,
                    task=node.task,
                    workdir=str(worktree.path),
                    pre_head=base,
                )
            )
            prompt = self._materialize_ctx(node, epoch, worktree)
            if prompt is None:
                self._fail_node(key, base, "unresolved or escaping context reference")
            try:
                result = self.registry.resolve(node.rig.name).run(prompt, worktree.path)
            except Exception as exc:
                self._fail_node(key, base, f"rig raised: {exc}")
            if not result.ok:
                self._record_failed_result(key, base, result)
            try:
                self.repo.commit_all(worktree.path, self._commit_message("do", key))
                candidate = self.repo.head(worktree.path)
                receipt = self.repo.receipt(base, candidate)
            except RepositoryError as exc:
                self._fail_node(key, base, f"do integration failed: {exc}")
            self._complete_attempt(key, base, candidate, result, receipt)
            return ExecutionOutcome(key=key)
        finally:
            self.repo.remove_worktree(worktree)
    def _exec_inplace(self, node: Inplace, epoch: int) -> ExecutionOutcome:
        key = (epoch, node.node_id)
        self._verify_resume_claims()
        base = self.repo.branch_tip()
        attempt = self._proj.node(key).dispatch_count
        worktree = self.repo.create_worktree(epoch, node.node_id, attempt, base)
        paths = [edit.path for edit in node.edits]
        try:
            self.journal.append(
                Dispatched(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=node.node_id,
                    task=f"inplace: {len(node.edits)} edit(s)",
                    workdir=str(worktree.path),
                    pre_head=base,
                )
            )
            try:
                for edit in node.edits:
                    target = self.repo.safe_path(worktree.path, edit.path, for_write=True)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(edit.content, encoding="utf-8")
                self.repo.commit_declared(
                    worktree.path, paths, self._commit_message("inplace", key)
                )
                candidate = self.repo.head(worktree.path)
                receipt = self.repo.receipt(base, candidate)
                undeclared = [path for path in receipt.paths if path not in set(paths)]
                if undeclared:
                    raise IntegrationError(
                        f"inplace commit contains undeclared paths: {', '.join(undeclared)}"
                    )
            except (OSError, UnicodeError, ValueError, RepositoryError) as exc:
                self._fail_node(key, base, f"inplace failed: {exc}")
            result = Result(
                text=(
                    f"wrote {', '.join(paths)}"
                    if receipt.commits
                    else "inplace: no diff (already applied)"
                ),
                files=paths if receipt.commits else [],
            )
            self._complete_attempt(key, base, candidate, result, receipt)
            return ExecutionOutcome(key=key)
        finally:
            self.repo.remove_worktree(worktree)
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
