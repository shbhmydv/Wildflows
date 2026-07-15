"""Standalone v2 frame supervisor and the three engine tool implementations."""
from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from wildflows.admission import AdmissionError, AdmissionPolicy, admit_dispatch
from wildflows.events import (
    Answered,
    Asked,
    DispatchCalled,
    DispatchReturned,
    FrameExited,
    FrameIntegrated,
    FrameIntegrating,
    FramePopped,
    FramePushed,
    GateCalled,
    GateReturned,
    RunFinished,
    RunOpened,
)
from wildflows.frame import (
    AskRequest,
    AskResult,
    ChildResult,
    DispatchRequest,
    DispatchResult,
    FrameOutcome,
    FrameResult,
    FrameRuntime,
    GateRequest,
    GateResult,
    ToolName,
    ToolRequest,
    ToolResponse,
    call_hash,
)
from wildflows.journal import Journal
from wildflows.mcp import MCPServer, ToolProtocolError
from wildflows.projection import FrameProjection, RunProjection
from wildflows.result import CommitReceipt
from wildflows.rig import RigRegistry, run_shell
from wildflows.shim import write_pi_shim
from wildflows.skill import SkillLibrary, SkillLibraryError
from wildflows.workspace import (
    FrameOwnershipError,
    FrameWorktree,
    IntegrationError,
    Repository,
    RepositoryError,
    RootIntegrationOwnershipError,
)


class CallConflictError(ToolProtocolError):
    """A frame reused a logical call index for different content."""


class FrameNotActiveError(ToolProtocolError):
    """A token-authenticated request did not name a live caller frame."""


@dataclass(frozen=True)
class _AdmissionHeadroom:
    remaining_depth: int
    max_parallel_width: int
    remaining_frames: int
    remaining_spend: float


class Engine:
    """One run's append owner, MCP service, frame stack, and serialized integrator."""

    ROOT_FRAME_ID = "f0"

    def __init__(
        self,
        run_dir: Path,
        workdir: Path,
        registry: RigRegistry,
        *,
        run_id: str,
        root_rig: str,
        root_prompt: str,
        run_branch: str | None = None,
        policy: AdmissionPolicy | None = None,
        worktrees_root: Path | None = None,
    ) -> None:
        self.run_dir = Path(run_dir).resolve()
        self.registry = registry
        self.run_id = run_id
        self._active: dict[str, FrameWorktree] = {}
        self._active_lock = threading.RLock()
        self._integration_lock = threading.RLock()
        self._workspace_lock = threading.RLock()
        self._admission_lock = threading.RLock()
        self._reservation_frames: dict[str, int] = {}
        self._reservation_spend: dict[str, float] = {}
        self._dispatch_reservations: dict[
            tuple[str, int], tuple[list[str], int, float]
        ] = {}
        self._answer_condition = threading.Condition(threading.RLock())
        self._call_condition = threading.Condition(threading.RLock())
        self._inflight_calls: dict[tuple[str, int], tuple[ToolName, str]] = {}
        journal_path = self.run_dir / "events.ndjson"
        continuing = journal_path.exists() and journal_path.stat().st_size > 0
        self.journal = Journal.load(self.run_dir) if continuing else Journal(self.run_dir)
        opened = self.journal.projection.opened
        if continuing and opened is None and self.journal.n_events == 0:
            # The only record was an uncertain torn run_opened. Repair accepted
            # no durable run identity or effect, so this run id is fresh again.
            continuing = False
        if continuing:
            if opened is None:
                raise RuntimeError("v2 journal has no run_opened event")
            if opened.run_id != run_id:
                raise ValueError("run id differs from durable run")
            if opened.root_prompt != root_prompt:
                raise ValueError("resumed job spec differs from durable root prompt")
            if opened.root_rig != root_rig:
                raise ValueError("resumed root rig differs from durable run")
            durable_worktrees = Path(opened.worktrees_root)
            if worktrees_root is not None and Path(worktrees_root).resolve() != durable_worktrees:
                raise ValueError("resumed worktrees root differs from durable run")
            self.policy = opened.policy
            self.repository = Repository(
                workdir,
                self.run_dir,
                run_id,
                run_branch=opened.run_branch,
                worktrees_root=durable_worktrees,
            )
            if str(self.repository.root) != opened.repository:
                raise ValueError("resumed repository differs from durable run")
            if run_branch is not None:
                requested = run_branch.removeprefix("refs/heads/")
                if requested != opened.run_branch:
                    raise ValueError("resumed run branch differs from durable run")
        else:
            self.policy = policy or AdmissionPolicy()
            self.repository = Repository(
                workdir,
                self.run_dir,
                run_id,
                run_branch=run_branch,
                worktrees_root=worktrees_root,
            )
            if root_rig not in registry:
                raise ValueError(f"root rig {root_rig!r} is not in the rig allowlist")
            started = time.time()
            self.journal.append(RunOpened(
                run_id=run_id,
                repository=str(self.repository.root),
                run_branch=self.repository.branch,
                base_commit=self.repository.branch_tip(),
                root_frame_id=self.ROOT_FRAME_ID,
                root_rig=root_rig,
                root_prompt=root_prompt,
                worktrees_root=str(self.repository.worktrees_root),
                started_at=started,
                policy=self.policy,
            ))
        self._sweep_integration_refs()
        if continuing:
            self._verify_integrations()
        self.skill_library = SkillLibrary(self.repository.root)
        self._restore_dispatch_reservations()
        self.server = MCPServer(self)

    @property
    def projection(self) -> RunProjection:
        return self.journal.projection

    def _sweep_integration_refs(self) -> None:
        with self.journal.projection_transaction() as projection:
            retained = {
                self.repository.integration_ref(frame.frame_id)
                for frame in projection.frames.values()
                if frame.integrating is not None
            }
        for ref in self.repository.integration_refs():
            if ref not in retained:
                self.repository.delete_integration_ref(ref)

    def _verify_integrations(self) -> None:
        for frame in self.journal.projection.frames.values():
            integrated = frame.integrated
            if integrated is None:
                continue
            self.repository.verify_receipt(
                integrated.integration_base, integrated.landed_commits
            )
            if integrated.landed_commits:
                target_ref = (
                    self.repository.ref
                    if integrated.target_frame_id is None
                    else self.journal.projection.frame(integrated.target_frame_id).branch
                )
                target_tip = self.repository.branch_tip(target_ref)
                candidate = integrated.candidate_head
                if self.repository.git(
                    ["merge-base", "--is-ancestor", candidate, target_tip], check=False
                ).returncode:
                    raise RepositoryError(
                        f"integrated frame {frame.frame_id!r} is absent from its target branch"
                    )

    def run(self) -> FrameResult:
        """Start or replay the root stack and return its terminal frame result."""
        finished = self.journal.projection.finished
        if finished is not None:
            return FrameResult(
                outcome=finished.outcome,
                text=finished.text,
                exit_code=0 if finished.outcome == "ok" else 1,
            )
        opened = self.journal.projection.opened
        assert opened is not None
        with self.server:
            root = self.journal.projection.frames.get(self.ROOT_FRAME_ID)
            if root is None or root.outcome is None:
                root = self._launch_frame(
                    frame_id=self.ROOT_FRAME_ID,
                    parent_frame_id=None,
                    parent_call_index=None,
                    task_index=None,
                    depth=0,
                    rig=opened.root_rig,
                    prompt=opened.root_prompt,
                    skills=[],
                    base_commit=opened.base_commit,
                    subtree_deadline=opened.started_at + self.policy.subtree_timeout_s,
                )
            if root.outcome == "ok" and root.integrated is None:
                try:
                    self._integrate_frame(root.frame_id, target_frame_id=None, owned=set())
                except RepositoryError as exc:
                    root.outcome = "failed"
                    root.text = f"root integration failed: {exc}"
            self._pop_once(root, root.outcome or "failed")
            root_head = root.head or self.repository.branch_tip(root.branch)
            outcome = root.outcome or "failed"
            self.journal.append(RunFinished(
                run_id=self.run_id,
                outcome=outcome,
                root_head=root_head,
                text=root.text,
            ))
            return FrameResult(
                outcome=outcome,
                text=root.text,
                exit_code=root.exit_code,
                stdout=root.stdout,
                stderr=root.stderr,
            )

    def handle_tool(
        self,
        frame_id: str,
        call_index: int,
        tool: ToolName,
        request: ToolRequest,
    ) -> ToolResponse:
        worktree = self._active_worktree(frame_id)
        frame = self.journal.projection.frame(frame_id)
        digest = call_hash(tool, request)
        key = (frame_id, call_index)
        with self._call_condition:
            while True:
                wait_for_leader = False
                with self.journal.projection_transaction() as projection:
                    existing = projection.call(frame_id, call_index)
                    if existing is not None:
                        if existing.tool != tool or existing.call_hash != digest:
                            raise CallConflictError(
                                f"call {frame_id}:{call_index} content differs from its durable call"
                            )
                        if existing.response is not None:
                            return existing.response
                    leader = self._inflight_calls.get(key)
                    if leader is not None:
                        if leader != (tool, digest):
                            raise CallConflictError(
                                f"call {frame_id}:{call_index} conflicts with its live call"
                            )
                        wait_for_leader = True
                    elif any(
                        owner == frame_id and index < call_index
                        for owner, index in self._inflight_calls
                    ):
                        wait_for_leader = True
                    else:
                        lower_pending = [
                            call
                            for (owner, index), call in projection.calls.items()
                            if owner == frame_id
                            and index < call_index
                            and not call.completed
                        ]
                        if lower_pending:
                            lower_live = any(
                                (frame_id, call.call_index) in self._inflight_calls
                                for call in lower_pending
                            )
                            if lower_live:
                                wait_for_leader = True
                            else:
                                raise CallConflictError(
                                    "an earlier durable call is pending; replay it before later calls"
                                )
                        elif (
                            existing is None
                            and call_index != projection.next_call_index(frame_id)
                        ):
                            raise CallConflictError(
                                f"call {frame_id}:{call_index} is not the next logical call"
                            )
                        else:
                            self._inflight_calls[key] = (tool, digest)
                            replaying = existing is not None
                            break
                if wait_for_leader:
                    self._call_condition.wait()
        try:
            if tool == "dispatch":
                if not isinstance(request, DispatchRequest):
                    raise TypeError("dispatch received the wrong request model")
                return self._dispatch(
                    frame, worktree, call_index, digest, request, replaying
                )
            if tool == "gate":
                if not isinstance(request, GateRequest):
                    raise TypeError("gate received the wrong request model")
                return self._gate(
                    frame, worktree, call_index, digest, request, replaying
                )
            if not isinstance(request, AskRequest):
                raise TypeError("ask received the wrong request model")
            return self._ask(
                frame, worktree, call_index, digest, request, replaying
            )
        finally:
            with self._call_condition:
                self._inflight_calls.pop(key, None)
                self._call_condition.notify_all()

    def _active_worktree(self, frame_id: str) -> FrameWorktree:
        with self._active_lock:
            try:
                return self._active[frame_id]
            except KeyError as exc:
                raise FrameNotActiveError(f"frame {frame_id!r} is not active") from exc

    def _dispatch(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: DispatchRequest,
        replaying: bool,
    ) -> DispatchResult:
        if replaying:
            self.repository.ensure_clean(worktree.path, frame.branch)
        else:
            caller_head = self.repository.ensure_clean(worktree.path, frame.branch)
            try:
                with self._admission_lock:
                    self._admit_and_reserve(frame, call_index, request)
                    self.journal.append(DispatchCalled(
                        run_id=self.run_id,
                        frame_id=frame.frame_id,
                        call_index=call_index,
                        call_hash=digest,
                        request=request,
                        caller_head=caller_head,
                    ))
            except AdmissionError as exc:
                self.journal.append(DispatchCalled(
                    run_id=self.run_id,
                    frame_id=frame.frame_id,
                    call_index=call_index,
                    call_hash=digest,
                    request=request,
                    caller_head=caller_head,
                ))
                refused = DispatchResult(
                    outcome="refused",
                    error_code=exc.code,
                    message=str(exc),
                )
                self.journal.append(DispatchReturned(
                    run_id=self.run_id,
                    frame_id=frame.frame_id,
                    call_index=call_index,
                    call_hash=digest,
                    result=refused,
                ))
                return refused

        try:
            for bundle in request.skills:
                self.skill_library.resolve(bundle)
        except SkillLibraryError as exc:
            self._clear_dispatch_reservation(frame.frame_id, call_index)
            failed = DispatchResult(outcome="failed", message=str(exc))
            self.journal.append(DispatchReturned(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                result=failed,
            ))
            return failed

        try:
            results = (
                self._parallel_children(frame, worktree, call_index, request)
                if request.parallel and len(request.tasks) > 1
                else self._serial_children(frame, worktree, call_index, request)
            )
        finally:
            self._clear_dispatch_reservation(frame.frame_id, call_index)
        outcome: FrameOutcome = (
            "ok" if all(result.outcome == "ok" for result in results) else "failed"
        )
        returned = DispatchResult(outcome=outcome, children=results)
        self.journal.append(DispatchReturned(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            result=returned,
        ))
        return returned

    def _ancestor_ids(self, frame: FrameProjection) -> list[str]:
        with self.journal.projection_transaction() as projection:
            ancestors: list[str] = []
            current: FrameProjection | None = frame
            while current is not None:
                ancestors.append(current.frame_id)
                current = (
                    None
                    if current.parent_frame_id is None
                    else projection.frame(current.parent_frame_id)
                )
            return ancestors

    def _subtree_admission_usage(self, frame_id: str) -> tuple[int, float]:
        with self.journal.projection_transaction() as projection:
            descendants = projection.descendants(frame_id)
            frames = len(descendants) + self._reservation_frames.get(frame_id, 0)
            spend = sum(self.policy.rig_cost(item.rig) for item in descendants)
            spend += self._reservation_spend.get(frame_id, 0.0)
            return frames, spend

    def _admission_headroom(self, frame: FrameProjection) -> _AdmissionHeadroom:
        with self.journal.projection_transaction():
            remaining_frames: list[int] = []
            remaining_spend: list[float] = []
            for ancestor_id in self._ancestor_ids(frame):
                frames, spend = self._subtree_admission_usage(ancestor_id)
                remaining_frames.append(self.policy.max_subtree_frames - frames)
                remaining_spend.append(self.policy.max_subtree_spend - spend)
            return _AdmissionHeadroom(
                remaining_depth=max(0, self.policy.max_depth - frame.depth),
                max_parallel_width=self.policy.max_breadth,
                remaining_frames=max(0, min(remaining_frames)),
                remaining_spend=max(0.0, min(remaining_spend)),
            )

    def _dispatchable_rig_names(
        self,
        frame: FrameProjection,
        headroom: _AdmissionHeadroom,
    ) -> tuple[str, ...]:
        if (
            headroom.remaining_depth == 0
            or headroom.remaining_frames == 0
            or time.time() >= frame.subtree_deadline
        ):
            return ()
        return tuple(
            name
            for name in self.registry.ordered_names
            if self.policy.rig_cost(name) <= headroom.remaining_spend
        )

    def _restore_dispatch_reservations(self) -> None:
        """Rebuild unconsumed admission reservations from durable pending calls."""
        with self.journal.projection_transaction() as projection:
            for call in projection.calls.values():
                if call.completed or call.tool != "dispatch":
                    continue
                request = call.request
                if not isinstance(request, DispatchRequest):
                    raise RuntimeError("dispatch call projection has the wrong request type")
                launched = {
                    child.task_index
                    for child in projection.frames.values()
                    if child.parent_frame_id == call.frame_id
                    and child.parent_call_index == call.call_index
                    and child.task_index is not None
                }
                remaining = len(request.tasks) - len(launched)
                if remaining <= 0:
                    continue
                frame = projection.frame(call.frame_id)
                ancestors = self._ancestor_ids(frame)
                spend = remaining * self.policy.rig_cost(request.rig)
                for ancestor_id in ancestors:
                    self._reservation_frames[ancestor_id] = (
                        self._reservation_frames.get(ancestor_id, 0) + remaining
                    )
                    self._reservation_spend[ancestor_id] = (
                        self._reservation_spend.get(ancestor_id, 0.0) + spend
                    )
                self._dispatch_reservations[(call.frame_id, call.call_index)] = (
                    ancestors, remaining, spend
                )

    def _admit_and_reserve(
        self,
        frame: FrameProjection,
        call_index: int,
        request: DispatchRequest,
    ) -> None:
        with self.journal.projection_transaction() as projection:
            ancestors = self._ancestor_ids(frame)
            for ancestor_id in ancestors:
                ancestor = projection.frame(ancestor_id)
                frames, spend = self._subtree_admission_usage(ancestor_id)
                admit_dispatch(
                    request,
                    caller_depth=frame.depth,
                    subtree_frames=frames,
                    subtree_spend=spend,
                    subtree_deadline=ancestor.subtree_deadline,
                    policy=self.policy,
                    registry=self.registry,
                )
            frames = len(request.tasks)
            spend = frames * self.policy.rig_cost(request.rig)
            for ancestor_id in ancestors:
                self._reservation_frames[ancestor_id] = (
                    self._reservation_frames.get(ancestor_id, 0) + frames
                )
                self._reservation_spend[ancestor_id] = (
                    self._reservation_spend.get(ancestor_id, 0.0) + spend
                )
            self._dispatch_reservations[(frame.frame_id, call_index)] = (
                ancestors, frames, spend
            )

    def _consume_dispatch_reservation(
        self, frame_id: str, call_index: int, rig: str
    ) -> None:
        with self._admission_lock:
            with self.journal.projection_transaction():
                key = (frame_id, call_index)
                reservation = self._dispatch_reservations.get(key)
                if reservation is None:
                    return
                ancestors, frames, spend = reservation
                unit = self.policy.rig_cost(rig)
                for ancestor_id in ancestors:
                    self._reservation_frames[ancestor_id] -= 1
                    self._reservation_spend[ancestor_id] -= unit
                remaining_frames = frames - 1
                remaining_spend = spend - unit
                if remaining_frames <= 0:
                    self._dispatch_reservations.pop(key, None)
                else:
                    self._dispatch_reservations[key] = (
                        ancestors, remaining_frames, remaining_spend
                    )

    def _clear_dispatch_reservation(self, frame_id: str, call_index: int) -> None:
        with self._admission_lock:
            with self.journal.projection_transaction():
                reservation = self._dispatch_reservations.pop(
                    (frame_id, call_index), None
                )
                if reservation is None:
                    return
                ancestors, frames, spend = reservation
                for ancestor_id in ancestors:
                    self._reservation_frames[ancestor_id] -= frames
                    self._reservation_spend[ancestor_id] -= spend

    def _serial_children(
        self,
        parent: FrameProjection,
        parent_worktree: FrameWorktree,
        call_index: int,
        request: DispatchRequest,
    ) -> list[ChildResult]:
        results: list[ChildResult] = []
        for task_index, task in enumerate(request.tasks):
            try:
                child = self._execute_child(
                    parent,
                    call_index,
                    task_index,
                    task,
                    request.rig,
                    request.skill_bundle(task_index),
                    base_commit=self.repository.branch_tip(parent.branch),
                )
                result = self._finish_child(
                    child, parent, parent_worktree, owned=set()
                )
            except Exception as exc:
                result = self._classify_child_exception(
                    parent.frame_id, call_index, task_index, exc
                )
            results.append(result)
        return results

    def _parallel_owned_paths(
        self, parent_frame_id: str, call_index: int
    ) -> set[str]:
        with self.journal.projection_transaction() as projection:
            owned: set[str] = set()
            for child in projection.frames.values():
                if (
                    child.parent_frame_id != parent_frame_id
                    or child.parent_call_index != call_index
                ):
                    continue
                claim = child.integrated or child.integrating
                if claim is not None:
                    owned.update(
                        path for commit in claim.source_commits for path in commit.paths
                    )
            return owned

    def _parallel_children(
        self,
        parent: FrameProjection,
        parent_worktree: FrameWorktree,
        call_index: int,
        request: DispatchRequest,
    ) -> list[ChildResult]:
        base = self.repository.branch_tip(parent.branch)
        by_index: dict[int, ChildResult] = {}
        with self.journal.projection_transaction() as projection:
            owned = self._parallel_owned_paths(parent.frame_id, call_index)
            launching = sum(
                1
                for task_index in range(len(request.tasks))
                if (
                    (existing := projection.frames.get(
                        self._child_id(parent.frame_id, call_index, task_index)
                    ))
                    is None
                    or existing.outcome is None
                )
            )
        start_barrier = threading.Barrier(launching) if launching > 1 else None
        with ThreadPoolExecutor(max_workers=len(request.tasks)) as executor:
            futures: dict[Future[FrameProjection], int] = {}
            for task_index, task in enumerate(request.tasks):
                future = executor.submit(
                    self._execute_child,
                    parent,
                    call_index,
                    task_index,
                    task,
                    request.rig,
                    request.skill_bundle(task_index),
                    base_commit=base,
                    start_barrier=start_barrier,
                )
                futures[future] = task_index
            for future in as_completed(futures):
                task_index = futures[future]
                try:
                    child = future.result()
                    result = self._finish_child(
                        child, parent, parent_worktree, owned=owned
                    )
                except Exception as exc:
                    result = self._classify_child_exception(
                        parent.frame_id, call_index, task_index, exc
                    )
                by_index[task_index] = result
        return [by_index[index] for index in range(len(request.tasks))]

    def _classify_child_exception(
        self,
        parent_frame_id: str,
        call_index: int,
        task_index: int,
        exc: Exception,
    ) -> ChildResult:
        """Memoize only failures proven to precede a durable frame push."""
        if self.journal.poisoned:
            raise exc
        frame_id = self._child_id(parent_frame_id, call_index, task_index)
        with self.journal.projection_transaction() as projection:
            pushed = frame_id in projection.frames
        if pushed:
            # Once a frame exists, an unexpected failure may follow an effect whose
            # outcome is not durable. Leave the dispatch pending for explicit replay.
            raise exc
        return ChildResult(
            frame_id=frame_id,
            outcome="failed",
            text=f"child launch failed before frame push: {exc}",
        )

    def _execute_child(
        self,
        parent: FrameProjection,
        call_index: int,
        task_index: int,
        task: str,
        rig: str,
        skills: list[str],
        *,
        base_commit: str,
        start_barrier: threading.Barrier | None = None,
    ) -> FrameProjection:
        frame_id = self._child_id(parent.frame_id, call_index, task_index)
        with self.journal.projection_transaction() as projection:
            existing = projection.frames.get(frame_id)
            if existing is not None:
                if (
                    existing.parent_frame_id != parent.frame_id
                    or existing.parent_call_index != call_index
                    or existing.task_index != task_index
                    or existing.prompt != task
                    or existing.rig != rig
                    or existing.skills != skills
                ):
                    raise CallConflictError(
                        f"durable child identity differs for {frame_id}"
                    )
                if existing.outcome is not None:
                    return existing
        return self._launch_frame(
            frame_id=frame_id,
            parent_frame_id=parent.frame_id,
            parent_call_index=call_index,
            task_index=task_index,
            depth=parent.depth + 1,
            rig=rig,
            prompt=task,
            skills=skills,
            base_commit=existing.base_commit if existing is not None else base_commit,
            subtree_deadline=parent.subtree_deadline,
            start_barrier=start_barrier,
        )

    @staticmethod
    def _child_id(parent: str, call_index: int, task_index: int) -> str:
        return f"{parent}.c{call_index}.t{task_index}"

    def _finish_child(
        self,
        child: FrameProjection,
        parent: FrameProjection,
        parent_worktree: FrameWorktree,
        *,
        owned: set[str],
    ) -> ChildResult:
        if child.outcome != "ok":
            self._pop_once(child, child.outcome or "failed")
            return self._child_result(child, [])
        try:
            with self._integration_lock:
                integrated = child.integrated
                if integrated is None:
                    integrated = self._integrate_frame(
                        child.frame_id,
                        target_frame_id=parent.frame_id,
                        owned=owned,
                        target_worktree=parent_worktree.path,
                    )
                owned.update(
                    path
                    for commit in integrated.source_commits
                    for path in commit.paths
                )
            self._pop_once(child, "ok")
            return self._child_result(child, integrated.landed_commits)
        except RepositoryError as exc:
            self._pop_once(child, "failed")
            return ChildResult(
                frame_id=child.frame_id,
                outcome="failed",
                text=f"integration failed: {exc}",
                exit_code=child.exit_code,
            )

    def _child_result(
        self, child: FrameProjection, commits: list[CommitReceipt]
    ) -> ChildResult:
        return ChildResult(
            frame_id=child.frame_id,
            outcome=child.outcome or "failed",
            text=child.text,
            exit_code=child.exit_code,
            commits=commits,
        )

    def _launch_frame(
        self,
        *,
        frame_id: str,
        parent_frame_id: str | None,
        parent_call_index: int | None,
        task_index: int | None,
        depth: int,
        rig: str,
        prompt: str,
        skills: list[str],
        base_commit: str,
        subtree_deadline: float,
        start_barrier: threading.Barrier | None = None,
    ) -> FrameProjection:
        with self.journal.projection_transaction() as projection:
            existing = projection.frames.get(frame_id)
            branch = (
                existing.branch
                if existing is not None
                else self.repository.frame_branch(frame_id)
            )
            if existing is None and self.repository.ref_exists(branch):
                raise FrameOwnershipError(
                    f"frame branch {branch!r} exists without a durable frame owner"
                )
            resume = existing is not None
        with self._workspace_lock:
            worktree = self.repository.create_frame_worktree(
                frame_id, branch, base_commit, resume=resume
            )
        attempt = 0 if existing is None else existing.push_count
        pushed = FramePushed(
            run_id=self.run_id,
            frame_id=frame_id,
            parent_frame_id=parent_frame_id,
            parent_call_index=parent_call_index,
            task_index=task_index,
            attempt=attempt,
            depth=depth,
            rig=rig,
            prompt=prompt,
            skills=list(skills),
            branch=branch,
            base_commit=base_commit,
            worktree=str(worktree.path),
            subtree_deadline=subtree_deadline,
        )
        if (
            existing is None
            and parent_frame_id is not None
            and parent_call_index is not None
        ):
            with self._admission_lock:
                self.journal.append(pushed)
                self._consume_dispatch_reservation(
                    parent_frame_id, parent_call_index, rig
                )
        else:
            self.journal.append(pushed)
        with self._active_lock:
            self._active[frame_id] = worktree
        capability = self.server.register_frame(frame_id)
        try:
            if start_barrier is not None:
                start_barrier.wait(timeout=30)
            runtime_dir = self.run_dir / "runtime" / frame_id / f"attempt-{attempt}"
            runtime_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(runtime_dir, 0o700)
            with self.journal.projection_transaction() as projection:
                replay_calls = [
                    (
                        call.call_index,
                        call.tool,
                        cast(
                            dict[str, object], call.request.model_dump(mode="json")
                        ),
                    )
                    for (owner, _), call in sorted(
                        projection.calls.items(), key=lambda item: item[0][1]
                    )
                    if owner == frame_id
                ]
                next_call_index = projection.next_call_index(frame_id)
            shim = write_pi_shim(
                runtime_dir,
                self.server.endpoint,
                capability,
                frame_id,
                next_call_index,
                replay_calls,
            )
            runtime = FrameRuntime(
                endpoint=self.server.endpoint,
                token=capability,
                frame_id=frame_id,
                shim_path=shim,
                runtime_dir=runtime_dir,
                next_call_index=next_call_index,
            )
            result = self.registry.resolve(rig).run(
                self._frame_prompt(frame_id, prompt, skills, worktree.path),
                worktree.path,
                runtime,
            )
            if result.outcome == "ok":
                try:
                    head = self.repository.commit_all(
                        worktree.path, f"wildflows frame {frame_id}"
                    )
                except RepositoryError as exc:
                    result = FrameResult(
                        outcome="failed",
                        text=f"frame commit failed: {exc}",
                        exit_code=result.exit_code,
                        stdout=result.stdout,
                        stderr=result.stderr,
                    )
                    head = self.repository.head(worktree.path)
            else:
                head = self.repository.head(worktree.path)
            self.journal.append(FrameExited(
                run_id=self.run_id,
                frame_id=frame_id,
                attempt=attempt,
                outcome=result.outcome,
                text=result.text,
                exit_code=result.exit_code,
                stdout=result.stdout,
                stderr=result.stderr,
                head=head,
            ))
            return self.journal.projection.frame(frame_id)
        except Exception as exc:
            head = self.repository.head(worktree.path)
            self.journal.append(FrameExited(
                run_id=self.run_id,
                frame_id=frame_id,
                attempt=attempt,
                outcome="failed",
                text=f"frame rig failed: {exc}",
                stderr=str(exc),
                head=head,
            ))
            return self.journal.projection.frame(frame_id)
        finally:
            self.server.revoke_frame(capability)
            with self._active_lock:
                self._active.pop(frame_id, None)
            with self._workspace_lock:
                self.repository.remove_worktree(worktree)

    def _frame_prompt(
        self,
        frame_id: str,
        original: str,
        assigned_skills: list[str],
        worktree: Path,
    ) -> str:
        skills = self.skill_library.resolve(assigned_skills)
        with self.journal.projection_transaction() as projection:
            calls = projection.resume_digest(frame_id)
            frame = projection.frame(frame_id)
            resume = bool(calls) or frame.push_count > 1
            headroom = self._admission_headroom(frame)
        progress_path = worktree / "progress.md"
        progress_note = (
            progress_path.read_text(encoding="utf-8")
            if resume and progress_path.is_file()
            else None
        )
        rig_lines: list[str] = []
        for name in self._dispatchable_rig_names(frame, headroom):
            description = self.registry.description(name)
            rig_lines.append(
                f"- {name}" if description is None else f"- {name}: {description}"
            )
        if not rig_lines:
            rig_lines.append("- (none within current admission headroom)")
        spend_unit = "unit" if headroom.remaining_spend == 1 else "units"
        preamble = (
            "--- RESOURCES ---\n"
            "You are a WILDFLOWS frame. Work only in your CWD. Commit useful changes "
            "before calling an engine tool or exiting.\n\n"
            "RIGS:\n"
            f"{'\n'.join(rig_lines)}\n"
            "Rig names are these registry keys; script filenames are not rig names.\n\n"
            "LIMITS:\n"
            f"- Remaining depth below this frame: {headroom.remaining_depth}.\n"
            f"- Maximum parallel width: {headroom.max_parallel_width} tasks per dispatch.\n"
            f"- Remaining descendant frame capacity: {headroom.remaining_frames}.\n"
            "- Remaining subtree spend capacity: "
            f"{headroom.remaining_spend:g} admission {spend_unit}.\n\n"
            f"{self.skill_library.manifest()}\n\n"
            "TOOLS:\n"
            "The only engine tools are wildflows_dispatch, wildflows_gate, and "
            "wildflows_ask. Tool calls block; child commits are present in your branch "
            "when dispatch returns. Dispatch skills is optional and contains one "
            "ordered skill-name list per task. Shapes are your control flow: a sequence "
            "is consecutive dispatch calls, a loop is redispatching until your own "
            "criterion is met, a fan-out is one dispatch with many tasks (parallel: "
            "true); combine these freely and choose per task. Prefer sequential "
            "dispatches when each task depends on the previous result; prefer parallel "
            "when tasks are independent. Admission refusals are durable, no-effect "
            "tool results: nothing was launched, and replay returns the same refusal for "
            "that call. Correct the request before making a new dispatch. Use "
            "wildflows_ask only when progress requires owner-only information or a "
            "decision; never use it to discover rigs, limits, or skills listed here.\n"
        )
        if resume:
            digest: dict[str, object] = {
                "calls": calls,
                "progress_note": progress_note,
            }
            preamble += (
                "\nRESUME REPLAY: completed calls below are durable and must not be paid "
                "for again. Do not re-issue completed calls; continue from their results. "
                "Re-issue an exact pending call to reconnect the durable stack. If a "
                "completed call is accidentally re-issued with its original logical index "
                "and content, the engine returns its memoized result.\n"
                f"RESUME_DIGEST={json.dumps(digest, sort_keys=True, separators=(',', ':'))}\n"
            )
        sections = [skill.text for skill in skills]
        sections.extend([
            f"--- FRAME JOB ---\n{original}",
            preamble,
        ])
        return "\n\n".join(sections)

    def _integrate_frame(
        self,
        frame_id: str,
        *,
        target_frame_id: str | None,
        owned: set[str],
        target_worktree: Path | None = None,
    ) -> FrameIntegrated:
        with self._integration_lock:
            frame = self.journal.projection.frame(frame_id)
            if frame.integrated is not None:
                return frame.integrated
            if frame.head is None:
                raise IntegrationError("cannot integrate a frame with no exit head")
            target_ref = (
                self.repository.ref
                if target_frame_id is None
                else self.journal.projection.frame(target_frame_id).branch
            )
            if target_frame_id is None:
                owner = self.repository.checked_out_owner(target_ref)
                if owner is not None and owner != self.repository.root:
                    raise RootIntegrationOwnershipError(
                        f"run branch {target_ref!r} is checked out by another "
                        f"worktree: {owner}"
                    )
                if (
                    target_worktree is not None
                    and target_worktree.resolve() != self.repository.root
                ):
                    raise RootIntegrationOwnershipError(
                        "root integration target is not the configured repository "
                        f"worktree: {target_worktree}"
                    )
                target_worktree = owner
            elif target_worktree is None:
                target_worktree = self.repository.checked_out_owner(target_ref)
            temporary_ref = self.repository.integration_ref(frame_id)
            intent = frame.integrating
            if intent is None:
                source = self.repository.receipt(frame.base_commit, frame.head)
                overlap = set(source.paths).intersection(owned)
                if overlap:
                    raise IntegrationError(
                        f"parallel sibling path ownership overlaps: {', '.join(sorted(overlap))}"
                    )
                moving_base = self.repository.branch_tip(target_ref)
                if moving_base == frame.base_commit:
                    candidate = frame.head
                    landed = source
                else:
                    candidate, landed = self.repository.reapply(
                        source.commits,
                        moving_base,
                        temporary_ref=temporary_ref,
                    )
                intent = FrameIntegrating(
                    run_id=self.run_id,
                    frame_id=frame_id,
                    target_frame_id=target_frame_id,
                    integration_base=moving_base,
                    candidate_head=candidate,
                    source_commits=source.commits,
                    landed_commits=landed.commits,
                )
                self.journal.append(intent)
            self.repository.advance(
                target_ref,
                intent.integration_base,
                intent.candidate_head,
                target_worktree=target_worktree,
            )
            integrated = FrameIntegrated(
                run_id=self.run_id,
                frame_id=frame_id,
                target_frame_id=target_frame_id,
                integration_base=intent.integration_base,
                candidate_head=intent.candidate_head,
                source_commits=intent.source_commits,
                landed_commits=intent.landed_commits,
            )
            self.journal.append(integrated)
            if self.repository.ref_exists(temporary_ref):
                self.repository.delete_integration_ref(temporary_ref)
            return integrated

    def _pop_once(self, frame: FrameProjection, outcome: FrameOutcome) -> None:
        if frame.popped:
            return
        self.journal.append(FramePopped(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            attempt=frame.attempt,
            outcome=outcome,
        ))

    def _gate(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: GateRequest,
        replaying: bool,
    ) -> GateResult:
        caller_head = self.repository.ensure_clean(worktree.path, frame.branch)
        if not replaying:
            self.journal.append(GateCalled(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                request=request,
                caller_head=caller_head,
            ))
        remaining = max(0.01, frame.subtree_deadline - time.time())
        result = run_shell(request.cmd, worktree.path, remaining)
        if result.timed_out:
            gate = GateResult(
                exit_code=124,
                stdout=result.stdout,
                stderr=result.stderr + f"\n[timeout] gate exceeded {remaining:g}s",
            )
        else:
            assert result.returncode is not None
            gate = GateResult(
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        self.journal.append(GateReturned(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            result=gate,
        ))
        return gate

    def _ask(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: AskRequest,
        replaying: bool,
    ) -> AskResult:
        self.repository.ensure_clean(worktree.path, frame.branch)
        if not replaying:
            self.journal.append(Asked(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                request=request,
            ))
        answer_path = self._answer_path(frame.frame_id, call_index)
        with self._answer_condition:
            while not answer_path.is_file():
                self._answer_condition.wait(timeout=0.25)
        answer = answer_path.read_text(encoding="utf-8")
        self.journal.append(Answered(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            answer=answer,
        ))
        return AskResult(answer=answer)

    def _answer_path(self, frame_id: str, call_index: int) -> Path:
        safe = frame_id.replace("/", "-")
        return self.run_dir / "answers" / f"{safe}-{call_index}.txt"

    def answer(
        self,
        answer: str,
        *,
        frame_id: str | None = None,
        call_index: int | None = None,
    ) -> tuple[str, int]:
        with self.journal.projection_transaction() as projection:
            pending = projection.pending_questions()
            selected = [
                call for call in pending
                if (frame_id is None or call.frame_id == frame_id)
                and (call_index is None or call.call_index == call_index)
            ]
            if len(selected) != 1:
                raise ValueError(
                    f"answer target is ambiguous or absent ({len(selected)} matches)"
                )
            call = selected[0]
        path = self._answer_path(call.frame_id, call.call_index)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(path.parent, 0o700)
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        descriptor = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(answer)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError as exc:
                raise ValueError("owner question already has an answer") from exc
        finally:
            temporary.unlink(missing_ok=True)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        with self._answer_condition:
            self._answer_condition.notify_all()
        return call.frame_id, call.call_index
