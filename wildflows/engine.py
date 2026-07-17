"""Standalone v2 frame supervisor and the three engine tool implementations."""
from __future__ import annotations

from contextlib import contextmanager
import json
import logging
import os
import signal
import subprocess as _subprocess
import threading
import time
from collections.abc import Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import cast

from wildflows.admission import AdmissionError, AdmissionPolicy, admit_dispatch
from wildflows.events import (
    Answered,
    Asked,
    CallFailed,
    CallRefused,
    DispatchCalled,
    DispatchReturned,
    FrameCommitWarning,
    FrameExited,
    FrameIntegrated,
    FrameIntegrating,
    FramePopped,
    FramePushed,
    FrameRelaunchBlocked,
    FrameSlotAcquired,
    FrameSlotQueued,
    FrameSlotReleased,
    GateCalled,
    GateReturned,
    RunFinished,
    RunInterrupted,
    RunOpened,
    WorkerReaped,
    WorktreeProvisioned,
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
    ToolFailure,
    ToolName,
    ToolRequest,
    ToolResponse,
    WorkerLease,
    call_hash,
    child_frame_id,
)
from wildflows.journal import Journal
from wildflows.mcp import MCPServer, ToolProtocolError, ValidatedToolCall
from wildflows.projection import FrameProjection, RunProjection
from wildflows.result import CommitReceipt
from wildflows.scheduler import RigSlotScheduler, SlotSchedulerStopped
from wildflows.rig import (
    RigRegistry,
    ScriptRig,
    WorkerReap,
    WorkerSupervisor,
    run_shell,
)
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


class _NotifierSubprocess:
    """Patchable notifier process seam, isolated from repository Git subprocesses."""

    Popen = staticmethod(_subprocess.Popen)
    DEVNULL = _subprocess.DEVNULL


subprocess = _NotifierSubprocess()
logger = logging.getLogger(__name__)


class CallConflictError(ToolProtocolError):
    """A frame reused a logical call index for different content."""


class FrameNotActiveError(ToolProtocolError):
    """A token-authenticated request did not name a live caller frame."""


class FrameCallJoinTimeoutError(RuntimeError):
    """A frame's effectful MCP worker did not acknowledge cancellation."""


class _EngineSignal(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__(signal.Signals(signum).name)


class FrameRelaunchBlockedError(RuntimeError):
    """An outcome-less frame branch contains effects the journal cannot explain."""

    def __init__(self, frame_id: str, expected_tip: str, found_tip: str) -> None:
        self.frame_id = frame_id
        self.expected_tip = expected_tip
        self.found_tip = found_tip
        super().__init__(
            f"frame {frame_id!r} relaunch parked: expected journal-explained tip "
            f"{expected_tip}, found {found_tip}; crash-window suspected. Operator: "
            "inspect the frame branch, manually disposition or reset the unexplained "
            "commit, then resume"
        )


FRAME_CALL_JOIN_TIMEOUT_S = 5.0
"""Maximum natural-return plus confirmed-cancellation wait at frame exit."""

FRAME_CALL_JOIN_RETRY_BACKOFF_S = (0.1, 0.25, 0.5)
"""Bounded waits after process-tree reaping before a call is failed durably."""

_EARLIER_ATTEMPT_LOG_LINES = 100
_EARLIER_ATTEMPT_LOG_BYTES = 16 * 1024
_EARLIER_ATTEMPT_DIFF_BYTES = 24 * 1024
_EARLIER_ATTEMPT_SUMMARY_BYTES = 8 * 1024
_EARLIER_ATTEMPT_BLOCK_BYTES = 64 * 1024
_EARLIER_ATTEMPT_REASON_BYTES = 512
_PROVISION_OUTPUT_BYTES = 16 * 1024


class WorktreeProvisioningError(RuntimeError):
    """A configured setup or link mechanism could not prepare a checkout."""


@dataclass(frozen=True)
class _LiveCall:
    tool: ToolName
    digest: str
    cancellation: threading.Event


@dataclass
class _FrameExecution:
    frame_id: str
    attempt: int
    rig: str
    budget_s: float
    used_s: float
    worker: WorkerLease
    lane: int | None = None
    lease_held: bool = False
    active_started: float | None = None
    lease_active_s: float = 0.0
    timer: threading.Timer | None = None
    generation: int = 0
    timed_out: bool = False
    closed: bool = False
    timeout_complete: threading.Event = field(default_factory=threading.Event)
    timeout_error: BaseException | None = None


@dataclass(frozen=True)
class _AdmissionHeadroom:
    remaining_depth: int
    max_parallel_width: int
    remaining_frames: int
    remaining_spend: float


class Engine:
    """One run's append owner, MCP service, frame stack, and serialized integrator."""

    ROOT_FRAME_ID = "f0"
    ROOT_SKILLS = ("dispatch-economy", "orchestration-shapes")

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
        notify_command: list[str] | None = None,
    ) -> None:
        self.run_dir = Path(run_dir).resolve()
        self.registry = registry
        self.run_id = run_id
        self._worktree_setup = registry.worktree_setup
        self._worktree_links = registry.worktree_links
        self._notify_command = tuple(notify_command or ())
        if notify_command is not None and not self._notify_command:
            raise ValueError("notify command must not be empty")
        self._active: dict[str, FrameWorktree] = {}
        self._active_lock = threading.RLock()
        self._execution_lock = threading.RLock()
        self._executions: dict[str, _FrameExecution] = {}
        self._slot_scheduler = RigSlotScheduler(registry.slot_capacities)
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
        self._inflight_calls: dict[tuple[str, int], _LiveCall] = {}
        self._terminating_frames: set[str] = set()
        self._call_context = threading.local()
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
            if opened.worktree_setup != self._worktree_setup:
                raise ValueError("resumed worktree setup differs from durable run")
            if tuple(opened.worktree_links) != self._worktree_links:
                raise ValueError("resumed worktree links differ from durable run")
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
                worktree_setup=self._worktree_setup,
                worktree_links=list(self._worktree_links),
                started_at=started,
                policy=self.policy,
            ))
        self._sweep_integration_refs()
        if continuing:
            self._verify_integrations()
        self.skill_library = SkillLibrary(self.repository.root)
        self._restore_dispatch_reservations()
        self._workers = WorkerSupervisor(self._record_worker_reap)
        self._sweep_outstanding_worker_handles()
        self._close_orphaned_slot_intervals()
        self.server = MCPServer(self)

    @property
    def projection(self) -> RunProjection:
        return self.journal.projection

    def _record_worker_reap(self, reaped: WorkerReap) -> None:
        self.journal.append(WorkerReaped(
            run_id=self.run_id,
            frame_id=reaped.frame_id,
            attempt=reaped.attempt,
            pid=reaped.pid,
            process_group_id=reaped.process_group_id,
            session_id=reaped.session_id,
            reason=reaped.reason,
            escalated=reaped.escalated,
        ))

    def _sweep_outstanding_worker_handles(self) -> None:
        reaped_attempts = {
            (event.frame_id, event.attempt)
            for event in self.journal.projection.effective_events
            if isinstance(event, WorkerReaped)
        }
        for frame in self.journal.projection.frames.values():
            key = (frame.frame_id, frame.attempt)
            if frame.outcome is not None or key in reaped_attempts:
                continue
            handle_path = (
                self.run_dir / "runtime" / frame.frame_id
                / f"attempt-{frame.attempt}" / "worker.handle"
            )
            if handle_path.is_file():
                self._workers.adopt_and_reap(
                    frame.frame_id,
                    frame.attempt,
                    handle_path,
                    "engine_resume_sweep",
                )

    @staticmethod
    def _gate_wait_s(
        events: Sequence[object], frame_id: str, started_at: float, ended_at: float
    ) -> float:
        returned = {
            (event.frame_id, event.call_index): event.ts
            for event in events
            if isinstance(event, GateReturned)
        }
        waited = 0.0
        for event in events:
            if (
                not isinstance(event, GateCalled)
                or event.frame_id != frame_id
                or event.ts < started_at
                or event.ts > ended_at
            ):
                continue
            returned_at = returned.get((event.frame_id, event.call_index), ended_at)
            waited += max(0.0, min(ended_at, returned_at) - event.ts)
        return min(waited, max(0.0, ended_at - started_at))

    def _close_orphaned_slot_intervals(self) -> None:
        """Stop an interrupted active interval before a frame is relaunched."""
        with self.journal.projection_transaction() as projection:
            active = [
                frame for frame in projection.frames.values() if frame.slot_active
            ]
            events = list(projection.effective_events)
        observed = time.time()
        for frame in active:
            acquired = next(
                event
                for event in reversed(events)
                if isinstance(event, FrameSlotAcquired)
                and event.frame_id == frame.frame_id
            )
            elapsed = max(0.0, observed - acquired.ts)
            gate_wait = self._gate_wait_s(
                events, frame.frame_id, acquired.ts, observed
            )
            self.journal.append(FrameSlotReleased(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                attempt=acquired.attempt,
                rig=acquired.rig,
                slot=acquired.slot,
                active_s=max(0.0, elapsed - gate_wait),
                reason="engine_resume_sweep",
            ))

    def _register_execution(
        self,
        frame_id: str,
        attempt: int,
        rig: str,
        worker: WorkerLease,
    ) -> _FrameExecution:
        with self.journal.projection_transaction() as projection:
            used_s = projection.frame(frame_id).attempt_self_time_s.get(
                attempt, 0.0
            )
        execution = _FrameExecution(
            frame_id=frame_id,
            attempt=attempt,
            rig=rig,
            budget_s=self.registry.resolve(rig).timeout_s,
            used_s=used_s,
            worker=worker,
        )
        with self._execution_lock:
            if frame_id in self._executions:
                raise RuntimeError(f"frame execution already registered for {frame_id!r}")
            self._executions[frame_id] = execution
        return execution

    def _acquire_frame_slot(
        self,
        execution: _FrameExecution,
        cancellation: threading.Event | None,
    ) -> bool:
        with self._execution_lock:
            if execution.closed or execution.timed_out:
                return False
            remaining = execution.budget_s - execution.used_s
        if remaining <= 0:
            with self._execution_lock:
                execution.timed_out = True
            try:
                execution.worker.stop("frame_self_timeout")
            finally:
                execution.timeout_complete.set()
            return False
        with self.journal.projection_transaction() as projection:
            previous = projection.frame(execution.frame_id).last_slot

        def queued() -> None:
            self.journal.append(FrameSlotQueued(
                run_id=self.run_id,
                frame_id=execution.frame_id,
                attempt=execution.attempt,
                rig=execution.rig,
            ))

        try:
            lane = self._slot_scheduler.acquire(
                execution.rig,
                execution.frame_id,
                previous=previous,
                cancellation=cancellation,
                on_queued=queued,
            )
        except SlotSchedulerStopped:
            return False
        try:
            self.journal.append(FrameSlotAcquired(
                run_id=self.run_id,
                frame_id=execution.frame_id,
                attempt=execution.attempt,
                rig=execution.rig,
                slot=lane,
            ))
        except BaseException:
            self._slot_scheduler.release(
                execution.rig, execution.frame_id, lane
            )
            raise
        with self._execution_lock:
            stopped = execution.closed or execution.timed_out
            if not stopped:
                execution.lane = lane
                execution.lease_held = True
                execution.active_started = time.monotonic()
                execution.lease_active_s = 0.0
                execution.generation += 1
                generation = execution.generation
                timer = threading.Timer(
                    remaining,
                    self._expire_frame_self_time,
                    args=(execution, generation),
                )
                timer.daemon = True
                execution.timer = timer
                timer.start()
        if stopped:
            self.journal.append(FrameSlotReleased(
                run_id=self.run_id,
                frame_id=execution.frame_id,
                attempt=execution.attempt,
                rig=execution.rig,
                slot=lane,
                active_s=0.0,
                reason="cancelled_before_start",
            ))
            self._slot_scheduler.release(
                execution.rig, execution.frame_id, lane
            )
            return False
        return True

    def _expire_frame_self_time(
        self, execution: _FrameExecution, generation: int
    ) -> None:
        with self._execution_lock:
            if (
                execution.closed
                or execution.timed_out
                or execution.active_started is None
                or execution.generation != generation
            ):
                return
            active_s = max(0.0, time.monotonic() - execution.active_started)
            execution.used_s += active_s
            execution.lease_active_s += active_s
            journalled_active_s = execution.lease_active_s
            lane = execution.lane
            execution.lane = None
            execution.lease_held = False
            execution.active_started = None
            execution.lease_active_s = 0.0
            execution.timer = None
            execution.timed_out = True
            execution.generation += 1
        try:
            execution.worker.stop("frame_self_timeout")
            try:
                self.journal.append(FrameSlotReleased(
                    run_id=self.run_id,
                    frame_id=execution.frame_id,
                    attempt=execution.attempt,
                    rig=execution.rig,
                    slot=lane,
                    active_s=journalled_active_s,
                    reason="self_time_timeout",
                ))
            finally:
                self._slot_scheduler.release(
                    execution.rig, execution.frame_id, lane
                )
        except BaseException as exc:
            with self._execution_lock:
                execution.timeout_error = exc
        finally:
            execution.timeout_complete.set()

    def _release_frame_slot(
        self, execution: _FrameExecution, reason: str, *, close: bool = False
    ) -> None:
        with self._execution_lock:
            if close:
                execution.closed = True
            if not execution.lease_held:
                if close:
                    self._executions.pop(execution.frame_id, None)
                return
            timer = execution.timer
            if timer is not None:
                timer.cancel()
            if execution.active_started is not None:
                active_s = max(
                    0.0, time.monotonic() - execution.active_started
                )
                execution.used_s += active_s
                execution.lease_active_s += active_s
            journalled_active_s = execution.lease_active_s
            lane = execution.lane
            execution.lane = None
            execution.lease_held = False
            execution.active_started = None
            execution.lease_active_s = 0.0
            execution.timer = None
            execution.generation += 1
            if close:
                self._executions.pop(execution.frame_id, None)
        try:
            self.journal.append(FrameSlotReleased(
                run_id=self.run_id,
                frame_id=execution.frame_id,
                attempt=execution.attempt,
                rig=execution.rig,
                slot=lane,
                active_s=journalled_active_s,
                reason=reason,
            ))
        finally:
            self._slot_scheduler.release(execution.rig, execution.frame_id, lane)

    def _pause_frame_self_time(self, frame_id: str) -> None:
        """Pause one caller clock without surrendering its resident slot."""
        with self._execution_lock:
            execution = self._executions.get(frame_id)
            if (
                execution is None
                or execution.closed
                or execution.timed_out
                or not execution.lease_held
                or execution.active_started is None
            ):
                return
            timer = execution.timer
            if timer is not None:
                timer.cancel()
            active_s = max(0.0, time.monotonic() - execution.active_started)
            execution.used_s += active_s
            execution.lease_active_s += active_s
            execution.active_started = None
            execution.timer = None
            execution.generation += 1

    def _resume_frame_self_time(self, frame_id: str) -> None:
        """Resume a gate-waiting caller clock on its retained slot."""
        with self._execution_lock:
            execution = self._executions.get(frame_id)
            if (
                execution is None
                or execution.closed
                or execution.timed_out
                or not execution.lease_held
                or execution.active_started is not None
            ):
                return
            remaining = max(0.0, execution.budget_s - execution.used_s)
            execution.active_started = time.monotonic()
            execution.generation += 1
            generation = execution.generation
            timer = threading.Timer(
                remaining,
                self._expire_frame_self_time,
                args=(execution, generation),
            )
            timer.daemon = True
            execution.timer = timer
            timer.start()

    def _park_frame(self, frame_id: str, reason: str) -> None:
        with self._execution_lock:
            execution = self._executions.get(frame_id)
        if execution is not None:
            self._release_frame_slot(execution, reason)

    def _resume_frame(
        self, frame_id: str, cancellation: threading.Event
    ) -> None:
        with self._execution_lock:
            execution = self._executions.get(frame_id)
        if execution is not None and not cancellation.is_set():
            self._acquire_frame_slot(execution, cancellation)

    def _close_frame_execution(self, frame_id: str, reason: str) -> None:
        with self._execution_lock:
            execution = self._executions.get(frame_id)
        if execution is not None:
            self._release_frame_slot(execution, reason, close=True)
            if execution.timed_out:
                execution.timeout_complete.wait()
                if execution.timeout_error is not None:
                    raise RuntimeError(
                        "frame self-time reap failed"
                    ) from execution.timeout_error

    @staticmethod
    def _slot_environment(lane: int | None) -> dict[str, str]:
        if lane is None:
            return {}
        return {
            "WILDFLOWS_SLOT": str(lane),
            "WILDFLOWS_PROVIDER_OVERRIDE": f"local-reviewer-{8081 + lane}",
        }

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

    @contextmanager
    def _signal_shutdown(self) -> Iterator[None]:
        if threading.current_thread() is not threading.main_thread():
            yield
            return
        watched = (signal.SIGINT, signal.SIGTERM)
        previous = {item: signal.getsignal(item) for item in watched}
        stopping = False

        def raise_signal(signum: int, frame: FrameType | None) -> None:
            nonlocal stopping
            del frame
            if stopping:
                return
            stopping = True
            raise _EngineSignal(signum)

        try:
            for item in watched:
                signal.signal(item, raise_signal)
            yield
        finally:
            for item, handler in previous.items():
                signal.signal(item, handler)

    def shutdown(self, reason: str) -> None:
        """Cancel live calls and synchronously reap every owned rig session."""
        with self._active_lock:
            frame_ids = set(self._active)
        with self._call_condition:
            frame_ids.update(owner for owner, _ in self._inflight_calls)
        for frame_id in frame_ids:
            self._cancel_frame_calls(frame_id)
        self._slot_scheduler.shutdown()
        self._workers.shutdown(reason)
        for frame_id in sorted(frame_ids):
            self._join_frame_calls(frame_id)
        with self._execution_lock:
            execution_ids = list(self._executions)
        for frame_id in execution_ids:
            self._close_frame_execution(frame_id, reason)

    @staticmethod
    def _fatal_reason(error: BaseException) -> str:
        detail = " ".join(str(error).split())
        summary = type(error).__name__ if not detail else f"{type(error).__name__}: {detail}"
        return f"fatal:{summary[:512]}"

    def _shutdown_preserving(self, reason: str, original: BaseException) -> None:
        try:
            self.shutdown(reason)
        except BaseException as cleanup_error:
            original.add_note(f"worker shutdown also failed: {cleanup_error!r}")
        try:
            self.journal.append(RunInterrupted(
                run_id=self.run_id,
                reason=reason,
            ))
        except BaseException as journal_error:
            original.add_note(
                f"run interruption journal append also failed: {journal_error!r}"
            )

    def run(self) -> FrameResult:
        """Drive the root stack with fatal and signal-safe worker shutdown."""
        with self._signal_shutdown():
            try:
                return self._drive_root()
            except _EngineSignal as exc:
                caught = signal.Signals(exc.signum)
                self._shutdown_preserving(f"signal:{caught.name}", exc)
                if caught == signal.SIGINT:
                    interrupted = KeyboardInterrupt()
                    interrupted.__notes__ = list(getattr(exc, "__notes__", ()))
                    raise interrupted from None
                stopped = SystemExit(128 + exc.signum)
                stopped.__notes__ = list(getattr(exc, "__notes__", ()))
                raise stopped from None
            except BaseException as exc:
                self._shutdown_preserving(self._fatal_reason(exc), exc)
                raise

    def _drive_root(self) -> FrameResult:
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
        self._raise_active_relaunch_block()
        with self.server:
            root = self.journal.projection.frames.get(self.ROOT_FRAME_ID)
            if root is None or root.outcome is None:
                root_skills = (
                    list(self.ROOT_SKILLS) if root is None else list(root.skills)
                )
                root = self._launch_frame(
                    frame_id=self.ROOT_FRAME_ID,
                    parent_frame_id=None,
                    parent_call_index=None,
                    task_index=None,
                    depth=0,
                    rig=opened.root_rig,
                    prompt=opened.root_prompt,
                    skills=root_skills,
                    base_commit=opened.base_commit,
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
                    refusal = projection.refusal(frame_id, call_index)
                    if existing is not None and (
                        existing.tool != tool or existing.call_hash != digest
                    ):
                        raise CallConflictError(
                            f"call {frame_id}:{call_index} content differs from its durable call"
                        )
                    if refusal is not None and (
                        refusal.tool != tool or refusal.call_hash != digest
                    ):
                        raise CallConflictError(
                            f"call {frame_id}:{call_index} content differs from its durable refusal"
                        )
                    leader = self._inflight_calls.get(key)
                    if leader is not None:
                        if leader.tool != tool or leader.digest != digest:
                            raise CallConflictError(
                                f"call {frame_id}:{call_index} conflicts with its live call"
                            )
                        wait_for_leader = True
                    elif existing is not None and existing.response is not None:
                        return existing.response
                    elif refusal is not None:
                        return ToolFailure(
                            error_code="call_refused", message=refusal.reason
                        )
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
                            cancellation = threading.Event()
                            if frame_id in self._terminating_frames:
                                cancellation.set()
                            live_call = _LiveCall(tool, digest, cancellation)
                            self._inflight_calls[key] = live_call
                            replaying = existing is not None
                            break
                if wait_for_leader:
                    self._call_condition.wait()
        self._call_context.cancellation = live_call.cancellation
        parked = tool in ("dispatch", "ask")
        try:
            if parked:
                self._park_frame(frame_id, tool)
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
        except Exception as exc:
            with self.journal.projection_transaction() as projection:
                called = projection.call(frame_id, call_index)
            if called is not None:
                raise
            if tool == "dispatch":
                self._clear_dispatch_reservation(frame_id, call_index)
            return self._record_call_refusal(
                frame_id, call_index, digest, tool, request, exc
            )
        finally:
            if parked:
                self._resume_frame(frame_id, live_call.cancellation)
            del self._call_context.cancellation
            with self._call_condition:
                self._inflight_calls.pop(key, None)
                self._call_condition.notify_all()

    def _record_call_refusal(
        self,
        frame_id: str,
        call_index: int,
        digest: str,
        tool: ToolName,
        request: ToolRequest,
        error: Exception,
    ) -> ToolFailure:
        reason = str(error).strip() or type(error).__name__
        with self.journal.projection_transaction() as projection:
            existing = projection.refusal(frame_id, call_index)
            if existing is None:
                self.journal.append(CallRefused(
                    run_id=self.run_id,
                    frame_id=frame_id,
                    call_index=call_index,
                    call_hash=digest,
                    tool=tool,
                    request=request,
                    reason=reason,
                ))
            elif existing.tool != tool or existing.call_hash != digest:
                raise CallConflictError(
                    f"call {frame_id}:{call_index} conflicts with its durable refusal"
                )
            else:
                reason = existing.reason
        logger.error(
            "call refused before %s_called for %s:%d: %s",
            tool,
            frame_id,
            call_index,
            reason,
        )
        return ToolFailure(error_code="call_refused", message=reason)

    def _current_call_cancellation(self) -> threading.Event:
        cancellation = getattr(self._call_context, "cancellation", None)
        if not isinstance(cancellation, threading.Event):
            raise RuntimeError("tool execution has no live call context")
        return cancellation

    def _cancel_frame_calls(self, frame_id: str) -> None:
        with self._call_condition:
            self._terminating_frames.add(frame_id)
            for (owner, _), call in self._inflight_calls.items():
                if owner == frame_id:
                    call.cancellation.set()
            self._call_condition.notify_all()
        with self._answer_condition:
            self._answer_condition.notify_all()

    def _durabilize_failed_calls(
        self,
        frame_id: str,
        calls: tuple[ValidatedToolCall, ...],
        *,
        error_code: str,
        message: str,
    ) -> None:
        unique: dict[tuple[int, str], ValidatedToolCall] = {}
        for call in calls:
            if call.frame_id == frame_id:
                unique[(call.call_index, call_hash(call.tool, call.request))] = call
        for (call_index, digest), call in unique.items():
            with self.journal.projection_transaction() as projection:
                refusal = projection.refusal(frame_id, call_index)
                if refusal is not None:
                    if refusal.tool != call.tool or refusal.call_hash != digest:
                        raise CallConflictError(
                            f"stopped call {frame_id}:{call_index} conflicts with its durable refusal"
                        )
                    continue
                existing = projection.call(frame_id, call_index)
                if existing is not None and existing.completed:
                    continue
                if existing is not None and (
                    existing.tool != call.tool or existing.call_hash != digest
                ):
                    raise CallConflictError(
                        f"stopped call {frame_id}:{call_index} conflicts with its durable call"
                    )
                self.journal.append(CallFailed(
                    run_id=self.run_id,
                    frame_id=frame_id,
                    call_index=call_index,
                    call_hash=digest,
                    tool=call.tool,
                    request=call.request,
                    result=ToolFailure(error_code=error_code, message=message),
                ))

    def _durabilize_stopped_calls(
        self, frame_id: str, calls: tuple[ValidatedToolCall, ...]
    ) -> None:
        self._durabilize_failed_calls(
            frame_id,
            calls,
            error_code="worker_stopped_without_return",
            message=(
                "validated MCP worker stopped without a durable "
                "tool-specific return"
            ),
        )

    def _join_frame_calls(self, frame_id: str) -> None:
        started = time.monotonic()
        natural_grace = FRAME_CALL_JOIN_TIMEOUT_S / 2
        joined = self.server.join_frame(frame_id, natural_grace)
        if joined.active:
            self._cancel_frame_calls(frame_id)
            remaining = max(
                0.0, FRAME_CALL_JOIN_TIMEOUT_S - (time.monotonic() - started)
            )
            joined = self.server.join_frame(frame_id, remaining)
        if joined.active:
            with self.journal.projection_transaction() as projection:
                frame_ids = {frame_id}
                frame_ids.update(
                    frame.frame_id for frame in projection.descendants(frame_id)
                )
            self._workers.reap_frames(frame_ids, "frame_call_join_timeout")
            for backoff in FRAME_CALL_JOIN_RETRY_BACKOFF_S:
                joined = self.server.join_frame(frame_id, backoff)
                if not joined.active:
                    break
        if joined.active:
            self._durabilize_failed_calls(
                frame_id,
                joined.active,
                error_code="frame_call_join_timeout",
                message=(
                    "validated MCP worker did not confirm execution stop after "
                    "process-tree reaping and join retries"
                ),
            )
            identities = ", ".join(
                f"{call.frame_id}:{call.call_index}" for call in joined.active
            )
            raise FrameCallJoinTimeoutError(
                "frame call join exceeded "
                f"{FRAME_CALL_JOIN_TIMEOUT_S:g}s without confirmed execution stop: "
                f"{identities}"
            )
        self._durabilize_stopped_calls(frame_id, joined.completed)

    def _active_worktree(self, frame_id: str) -> FrameWorktree:
        with self._active_lock:
            try:
                return self._active[frame_id]
            except KeyError as exc:
                raise FrameNotActiveError(f"frame {frame_id!r} is not active") from exc

    def _append_tool_return(
        self, event: DispatchReturned | GateReturned | Answered
    ) -> None:
        with self.journal.projection_transaction() as projection:
            call = projection.call(event.frame_id, event.call_index)
            if call is not None and call.completed:
                return
            self.journal.append(event)

    def _dispatch(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: DispatchRequest,
        replaying: bool,
    ) -> DispatchResult:
        if request.retry_frame is not None:
            return self._dispatch_retry(
                frame, worktree, call_index, digest, request, replaying
            )
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
                if exc.code == "rig_not_allowed":
                    raise
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
                self._append_tool_return(DispatchReturned(
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
            self._append_tool_return(DispatchReturned(
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
        self._append_tool_return(DispatchReturned(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            result=returned,
        ))
        return returned

    def _retry_refused(
        self,
        frame: FrameProjection,
        call_index: int,
        digest: str,
        code: str,
        message: str,
    ) -> DispatchResult:
        refused = DispatchResult(
            outcome="refused", error_code=code, message=message
        )
        self._append_tool_return(DispatchReturned(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            result=refused,
        ))
        return refused

    def _dispatch_retry(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: DispatchRequest,
        replaying: bool,
    ) -> DispatchResult:
        retry_frame = request.retry_frame
        assert retry_frame is not None
        caller_head = self.repository.ensure_clean(worktree.path, frame.branch)
        if not replaying:
            self.journal.append(DispatchCalled(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                request=request,
                caller_head=caller_head,
            ))
        with self.journal.projection_transaction() as projection:
            child = projection.frames.get(retry_frame)
            direct_child = (
                child is not None and child.parent_frame_id == frame.frame_id
            )
            retryable = (
                child is not None
                and (
                    child.outcome == "failed"
                    or (replaying and child.outcome in (None, "ok"))
                )
            )
            durable_call = projection.call(frame.frame_id, call_index)
            retry_attempt_started = (
                replaying
                and durable_call is not None
                and any(
                    isinstance(event, FramePushed)
                    and event.frame_id == retry_frame
                    and event.seq > durable_call.started_seq
                    for event in projection.effective_events
                )
            )
        if not direct_child or child is None:
            return self._retry_refused(
                frame,
                call_index,
                digest,
                "retry_not_direct_child",
                f"frame {retry_frame!r} is not a direct child of {frame.frame_id!r}",
            )
        if not retryable:
            return self._retry_refused(
                frame,
                call_index,
                digest,
                "retry_child_not_failed",
                f"frame {retry_frame!r} is not a failed direct child",
            )
        cancellation = self._current_call_cancellation()
        if child.outcome is None or (
            child.outcome == "failed" and not retry_attempt_started
        ):
            temporary_ref = self.repository.integration_ref(child.frame_id)
            if child.integrating is not None and self.repository.ref_exists(temporary_ref):
                self.repository.delete_integration_ref(temporary_ref)
            child = self._launch_frame(
                frame_id=child.frame_id,
                parent_frame_id=child.parent_frame_id,
                parent_call_index=child.parent_call_index,
                task_index=child.task_index,
                depth=child.depth,
                rig=child.rig,
                prompt=child.prompt,
                skills=list(child.skills),
                base_commit=child.base_commit,
                cancellation=cancellation,
            )
        result = self._finish_child(child, frame, worktree, owned=set())
        returned = DispatchResult(
            outcome="ok" if result.outcome == "ok" else "failed",
            children=[result],
        )
        self._append_tool_return(DispatchReturned(
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
        if headroom.remaining_depth == 0 or headroom.remaining_frames == 0:
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
                remaining_indexes = [
                    index for index in range(len(request.tasks))
                    if index not in launched
                ]
                remaining = len(remaining_indexes)
                if remaining <= 0:
                    continue
                frame = projection.frame(call.frame_id)
                task_rigs = self.registry.task_rigs(
                    request.rig, frame.rig, len(request.tasks)
                )
                ancestors = self._ancestor_ids(frame)
                spend = sum(
                    self.policy.rig_cost(task_rigs[index])
                    for index in remaining_indexes
                )
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
            task_rigs: tuple[str, ...] = ()
            for ancestor_id in ancestors:
                ancestor = projection.frame(ancestor_id)
                frames, spend = self._subtree_admission_usage(ancestor_id)
                task_rigs = admit_dispatch(
                    request,
                    caller_depth=frame.depth,
                    caller_rig=frame.rig,
                    subtree_frames=frames,
                    subtree_spend=spend,
                    policy=self.policy,
                    registry=self.registry,
                )
            frames = len(request.tasks)
            spend = sum(self.policy.rig_cost(rig) for rig in task_rigs)
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
        cancellation = self._current_call_cancellation()
        task_rigs = self.registry.task_rigs(
            request.rig, parent.rig, len(request.tasks)
        )
        for task_index, task in enumerate(request.tasks):
            try:
                child = self._execute_child(
                    parent,
                    call_index,
                    task_index,
                    task,
                    task_rigs[task_index],
                    request.skill_bundle(task_index),
                    base_commit=self.repository.branch_tip(parent.branch),
                    cancellation=cancellation,
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
        cancellation = self._current_call_cancellation()
        task_rigs = self.registry.task_rigs(
            request.rig, parent.rig, len(request.tasks)
        )
        by_index: dict[int, ChildResult] = {}
        with self.journal.projection_transaction() as projection:
            owned = self._parallel_owned_paths(parent.frame_id, call_index)
            launching = sum(
                1
                for task_index in range(len(request.tasks))
                if (
                    (existing := projection.frames.get(
                        child_frame_id(parent.frame_id, call_index, task_index)
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
                    task_rigs[task_index],
                    request.skill_bundle(task_index),
                    base_commit=base,
                    start_barrier=start_barrier,
                    cancellation=cancellation,
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
        frame_id = child_frame_id(parent_frame_id, call_index, task_index)
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
        cancellation: threading.Event | None = None,
    ) -> FrameProjection:
        frame_id = child_frame_id(parent.frame_id, call_index, task_index)
        if cancellation is not None and cancellation.is_set():
            raise RuntimeError("dispatch cancelled before child frame push")
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
            start_barrier=start_barrier,
            cancellation=cancellation,
        )

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
            failed = self._child_result(child, [])
            return failed.model_copy(update={"text": f"integration failed: {exc}"})

    def _child_result(
        self, child: FrameProjection, commits: list[CommitReceipt]
    ) -> ChildResult:
        outcome = child.outcome or "failed"
        head = child.head
        failed = outcome != "ok" and head is not None
        return ChildResult(
            frame_id=child.frame_id,
            outcome=outcome,
            text=child.text,
            exit_code=child.exit_code,
            commits=commits,
            branch=(
                child.branch.removeprefix("refs/heads/") if failed else None
            ),
            head=head if failed else None,
            diffstat=(
                self.repository.diffstat(child.base_commit, head)
                if failed and head is not None
                else None
            ),
        )

    def _explained_frame_tips(
        self, projection: RunProjection, frame_id: str, branch_tip: str
    ) -> tuple[str, set[str]]:
        """Return the canonical and currently allowed journal-explained tips."""
        frame = projection.frame(frame_id)
        completed_tip = frame.base_commit
        pending: FrameIntegrating | None = None

        def advance_observed_tip(observed: str, *, require_on_branch: bool) -> bool:
            nonlocal completed_tip
            if observed == completed_tip:
                return True
            if not self.repository.is_ancestor(completed_tip, observed):
                return False
            if require_on_branch and not self.repository.is_ancestor(
                observed, branch_tip
            ):
                return False
            completed_tip = observed
            return True

        for event in projection.effective_events:
            if (
                isinstance(event, (DispatchCalled, GateCalled, Asked))
                and event.frame_id == frame_id
            ):
                caller_head = event.caller_head
                if caller_head is not None and pending is None:
                    advance_observed_tip(caller_head, require_on_branch=False)
            elif (
                isinstance(event, FrameExited)
                and event.frame_id == frame_id
                and pending is None
            ):
                advance_observed_tip(event.head, require_on_branch=True)
            elif (
                isinstance(event, FrameIntegrating)
                and event.target_frame_id == frame_id
                and pending is None
                and advance_observed_tip(
                    event.integration_base, require_on_branch=True
                )
            ):
                pending = event
            elif (
                isinstance(event, FrameIntegrated)
                and event.target_frame_id == frame_id
            ):
                matches_pending = (
                    pending is not None
                    and event.integration_base == pending.integration_base
                    and event.candidate_head == pending.candidate_head
                )
                starts_at_completed_tip = event.integration_base == completed_tip
                if (
                    not starts_at_completed_tip
                    and pending is None
                    and advance_observed_tip(
                        event.integration_base, require_on_branch=True
                    )
                ):
                    starts_at_completed_tip = True
                if starts_at_completed_tip or matches_pending:
                    completed_tip = event.candidate_head
                    pending = None
        if pending is None:
            return completed_tip, {completed_tip}
        return pending.candidate_head, {
            pending.integration_base,
            pending.candidate_head,
        }

    def _guard_frame_relaunch(
        self, projection: RunProjection, frame: FrameProjection
    ) -> None:
        found_tip = self.repository.branch_tip(frame.branch)
        expected_tip, allowed = self._explained_frame_tips(
            projection, frame.frame_id, found_tip
        )
        if found_tip in allowed:
            return
        error = FrameRelaunchBlockedError(
            frame.frame_id, expected_tip, found_tip
        )
        blocked = frame.relaunch_blocked
        if (
            blocked is None
            or blocked.expected_tip != expected_tip
            or blocked.found_tip != found_tip
        ):
            self.journal.append(FrameRelaunchBlocked(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                expected_tip=expected_tip,
                found_tip=found_tip,
                message=str(error),
            ))
        raise error

    def _raise_active_relaunch_block(self) -> None:
        with self.journal.projection_transaction() as projection:
            blocked = [
                frame
                for frame in projection.frames.values()
                if frame.outcome is None and frame.relaunch_blocked is not None
            ]
            for frame in blocked:
                self._guard_frame_relaunch(projection, frame)

    @staticmethod
    def _truncate_utf8(value: str, limit: int, marker: str) -> str:
        encoded = value.encode("utf-8")
        if len(encoded) <= limit:
            return value
        suffix = f"\n{marker}".encode("utf-8")
        available = max(0, limit - len(suffix))
        prefix = encoded[:available].decode("utf-8", errors="ignore")
        return prefix + suffix.decode("utf-8")

    @staticmethod
    def _tail_attempt_log(path: Path, stream_name: str) -> str:
        marker = f"[... PRIOR {stream_name.upper()} TRUNCATED ...]\n"
        try:
            with path.open("rb") as stream:
                stream.seek(0, os.SEEK_END)
                size = stream.tell()
                read_size = min(size, _EARLIER_ATTEMPT_LOG_BYTES)
                if read_size:
                    stream.seek(-read_size, os.SEEK_END)
                data = stream.read(read_size)
        except OSError as exc:
            return f"[prior {stream_name} log unavailable: {exc}]"
        truncated = size > len(data)
        if truncated:
            # A byte-bounded seek may begin midway through a line. Keep only
            # the complete tail when another newline remains in the window.
            first_newline = data.find(b"\n")
            if first_newline >= 0:
                data = data[first_newline + 1:]
        lines = data.splitlines(keepends=True)
        if len(lines) > _EARLIER_ATTEMPT_LOG_LINES:
            lines = lines[-_EARLIER_ATTEMPT_LOG_LINES:]
            truncated = True
        body = b"".join(lines).decode("utf-8", errors="replace")
        if not body:
            body = "(empty)"
        if not truncated:
            return body
        marker_bytes = marker.encode("utf-8")
        body_bytes = body.encode("utf-8")
        body_budget = _EARLIER_ATTEMPT_LOG_BYTES - len(marker_bytes)
        if len(body_bytes) > body_budget:
            body = body_bytes[-body_budget:].decode("utf-8", errors="ignore")
        return marker + body

    def _attempt_log_tail(
        self, frame: FrameProjection, stream_name: str
    ) -> tuple[str, str]:
        filenames = (
            f"pi.{stream_name}.log",
            f"agent.{stream_name}.log",
        )
        roots: list[Path] = []
        rig = (
            self.registry.resolve(frame.rig)
            if frame.rig in self.registry
            else None
        )
        if isinstance(rig, ScriptRig):
            expected = rig.log_dir / Path(frame.worktree).name
            roots.append(expected)
            try:
                siblings = list(rig.log_dir.iterdir())
            except OSError:
                siblings = []
            prefix = f"{expected.name}-"
            suffixed = sorted(
                (
                    candidate
                    for candidate in siblings
                    if candidate.is_dir()
                    and candidate.name.startswith(prefix)
                    and candidate.name.removeprefix(prefix).isdigit()
                ),
                key=lambda candidate: int(candidate.name.removeprefix(prefix)),
                reverse=True,
            )
            roots.extend(suffixed)
        roots.append(
            self.run_dir / "runtime" / frame.frame_id
            / f"attempt-{frame.attempt}"
        )
        for root in roots:
            for filename in filenames:
                path = root / filename
                if path.is_file():
                    return filename, self._tail_attempt_log(path, stream_name)
        return f"(no prior {stream_name} log found)", "(unavailable)"

    @staticmethod
    def _bounded_command_output(
        argv: list[str], cwd: Path, limit: int
    ) -> tuple[bytes, bool, int]:
        try:
            process = _subprocess.Popen(
                argv,
                cwd=cwd,
                stdout=_subprocess.PIPE,
                stderr=_subprocess.DEVNULL,
            )
        except OSError:
            return b"", False, 127
        assert process.stdout is not None
        output = process.stdout.read(limit + 1)
        truncated = len(output) > limit
        if truncated:
            process.kill()
            output = output[:limit]
        returncode = process.wait()
        return output, truncated, returncode

    def _diff_summary(self, worktree: Path) -> str:
        stat, stat_truncated, stat_code = self._bounded_command_output(
            [
                "git",
                "diff",
                "--stat",
                "--no-ext-diff",
                "--no-textconv",
                "HEAD",
                "--",
                ".",
            ],
            worktree,
            _EARLIER_ATTEMPT_SUMMARY_BYTES,
        )
        status, status_truncated, status_code = self._bounded_command_output(
            ["git", "status", "--short", "--untracked-files=all"],
            worktree,
            _EARLIER_ATTEMPT_SUMMARY_BYTES,
        )
        stat_text = (
            stat.decode("utf-8", errors="replace").rstrip()
            if stat_code in (0, -9)
            else "(unavailable)"
        )
        status_text = (
            status.decode("utf-8", errors="replace").rstrip()
            if status_code in (0, -9)
            else "(unavailable)"
        )
        if stat_truncated:
            stat_text += "\n[... DIFF STAT TRUNCATED ...]"
        if status_truncated:
            status_text += "\n[... STATUS TRUNCATED ...]"
        stat_text = self._truncate_utf8(
            stat_text,
            _EARLIER_ATTEMPT_SUMMARY_BYTES,
            "[... DIFF STAT TRUNCATED ...]",
        )
        status_text = self._truncate_utf8(
            status_text,
            _EARLIER_ATTEMPT_SUMMARY_BYTES,
            "[... STATUS TRUNCATED ...]",
        )
        return (
            "[... UNCOMMITTED DIFF OMITTED: exceeded bounded evidence limit; "
            "stat/status summary follows ...]\n"
            f"git diff --stat HEAD:\n{stat_text or '(no tracked diff stat)'}\n"
            f"git status --short:\n{status_text or '(clean)'}"
        )

    def _uncommitted_diff(self, worktree: Path) -> str:
        if not worktree.is_dir():
            return "(prior attempt worktree is unavailable)"
        tracked, tracked_truncated, tracked_code = self._bounded_command_output(
            [
                "git",
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--no-color",
                "--binary",
                "HEAD",
                "--",
                ".",
            ],
            worktree,
            _EARLIER_ATTEMPT_DIFF_BYTES,
        )
        if tracked_truncated:
            return self._diff_summary(worktree)
        if tracked_code != 0:
            return f"(uncommitted diff unavailable: git exited {tracked_code})"

        untracked, names_truncated, names_code = self._bounded_command_output(
            ["git", "ls-files", "--others", "--exclude-standard", "-z"],
            worktree,
            _EARLIER_ATTEMPT_DIFF_BYTES,
        )
        if names_truncated:
            return self._diff_summary(worktree)
        if names_code != 0:
            return f"(uncommitted diff unavailable: git exited {names_code})"

        combined = bytearray(tracked)
        for raw_name in untracked.split(b"\0"):
            if not raw_name:
                continue
            separator = b"\n" if combined and not combined.endswith(b"\n") else b""
            remaining = (
                _EARLIER_ATTEMPT_DIFF_BYTES - len(combined) - len(separator)
            )
            if remaining <= 0:
                return self._diff_summary(worktree)
            name = os.fsdecode(raw_name)
            patch, patch_truncated, patch_code = self._bounded_command_output(
                [
                    "git",
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--no-color",
                    "--binary",
                    "--no-index",
                    "--",
                    "/dev/null",
                    name,
                ],
                worktree,
                remaining,
            )
            if patch_truncated or patch_code not in (0, 1):
                return self._diff_summary(worktree)
            if patch:
                combined.extend(separator)
                combined.extend(patch)
        if not combined:
            return "(clean; no uncommitted diff)"
        decoded = combined.decode("utf-8", errors="replace").rstrip()
        if len(decoded.encode("utf-8")) > _EARLIER_ATTEMPT_DIFF_BYTES:
            return self._diff_summary(worktree)
        return decoded

    def _attempt_dirty_diff(self, frame: FrameProjection) -> str:
        worktree = Path(frame.worktree)
        if worktree.is_dir():
            return self._uncommitted_diff(worktree)
        snapshot = (
            self.run_dir
            / "runtime"
            / frame.frame_id
            / f"attempt-{frame.attempt}"
            / "uncommitted.diff"
        )
        try:
            return snapshot.read_text(encoding="utf-8")
        except OSError:
            return "(prior attempt worktree is unavailable)"

    def _snapshot_failed_worktree(
        self, frame_id: str, attempt: int, worktree: Path
    ) -> None:
        evidence = self._uncommitted_diff(worktree)
        path = (
            self.run_dir
            / "runtime"
            / frame_id
            / f"attempt-{attempt}"
            / "uncommitted.diff"
        )
        try:
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            path.write_text(evidence, encoding="utf-8")
        except OSError:
            # Relaunch evidence is best effort and cannot change frame disposition.
            return

    def _attempt_death_reason(self, frame: FrameProjection) -> str:
        with self.journal.projection_transaction() as projection:
            reaped = [
                event
                for event in projection.effective_events
                if isinstance(event, WorkerReaped)
                and event.frame_id == frame.frame_id
                and event.attempt == frame.attempt
            ]
        if frame.outcome is not None:
            exit_detail = (
                f"exit={frame.exit_code}" if frame.exit_code is not None
                else f"outcome={frame.outcome}"
            )
            text = self._truncate_utf8(
                frame.text.replace("\r", " ").replace("\n", " "),
                _EARLIER_ATTEMPT_REASON_BYTES,
                "[... REASON TRUNCATED ...]",
            )
            suffix = "" if not text else f": {text}"
            return f"{frame.outcome} ({exit_detail}){suffix}"
        if not reaped:
            return "crash (no worker_reaped cause was recorded)"
        raw_reason = self._truncate_utf8(
            reaped[-1].reason.replace("\r", " ").replace("\n", " "),
            _EARLIER_ATTEMPT_REASON_BYTES,
            "[... REASON TRUNCATED ...]",
        )
        lowered = raw_reason.lower()
        if "timeout" in lowered:
            category = "timeout"
        elif raw_reason.startswith("signal:"):
            category = "operator reset/stop"
        elif raw_reason == "engine_resume_sweep" or raw_reason.startswith("fatal:"):
            category = "crash"
        else:
            category = "worker_reaped"
        return f"{category} (worker_reaped reason={raw_reason})"

    def _earlier_attempt_block(self, frame: FrameProjection) -> str:
        stdout_name, stdout_tail = self._attempt_log_tail(frame, "stdout")
        stderr_name, stderr_tail = self._attempt_log_tail(frame, "stderr")
        dirty_diff = self._attempt_dirty_diff(frame)
        block = (
            "--- EARLIER ATTEMPT ---\n"
            f"This is relaunch attempt {frame.push_count + 1} for frame "
            f"{frame.frame_id}.\n"
            f"Earlier attempt {frame.attempt + 1} died: "
            f"{self._attempt_death_reason(frame)}.\n"
            "Use this bounded evidence to continue rather than re-deriving prior "
            "work. Logs are tails, not complete transcripts.\n\n"
            f"--- PRIOR STDOUT ({stdout_name}) ---\n{stdout_tail}\n\n"
            f"--- PRIOR STDERR ({stderr_name}) ---\n{stderr_tail}\n\n"
            f"--- UNCOMMITTED WORKTREE DIFF ---\n{dirty_diff}\n"
            "--- END EARLIER ATTEMPT ---"
        )
        return self._truncate_utf8(
            block,
            _EARLIER_ATTEMPT_BLOCK_BYTES,
            "[... EARLIER ATTEMPT BLOCK TRUNCATED ...]",
        )

    @staticmethod
    def _provision_output_tail(stdout: str, stderr: str) -> str:
        combined = f"--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
        encoded = combined.encode("utf-8")
        if len(encoded) <= _PROVISION_OUTPUT_BYTES:
            return combined
        marker = b"[... PROVISIONING OUTPUT TRUNCATED ...]\n"
        tail = encoded[-(_PROVISION_OUTPUT_BYTES - len(marker)):]
        return (marker + tail).decode("utf-8", errors="ignore")

    def _provisioning_recorded(
        self, worktree: FrameWorktree, mechanism: str
    ) -> bool:
        with self.journal.projection_transaction() as projection:
            return any(
                isinstance(event, WorktreeProvisioned)
                and event.worktree == str(worktree.path)
                and event.mechanism == mechanism
                and event.outcome in ("ok", "skipped")
                for event in projection.effective_events
            )

    def _provision_setup(
        self, worktree: FrameWorktree, attempt: int, command: str
    ) -> None:
        if self._provisioning_recorded(worktree, "setup"):
            return
        started = time.monotonic()
        try:
            result = run_shell(command, worktree.path, None)
        except (OSError, _subprocess.SubprocessError) as exc:
            duration_s = max(0.0, time.monotonic() - started)
            detail = f"worktree setup could not launch: {exc}"
            self.journal.append(WorktreeProvisioned(
                run_id=self.run_id,
                frame_id=worktree.frame_id,
                attempt=attempt,
                worktree=str(worktree.path),
                mechanism="setup",
                duration_s=duration_s,
                outcome="failed",
                output_tail=detail,
            ))
            raise WorktreeProvisioningError(detail) from exc
        duration_s = max(0.0, time.monotonic() - started)
        assert result.returncode is not None
        output_tail = self._provision_output_tail(result.stdout, result.stderr)
        self.journal.append(WorktreeProvisioned(
            run_id=self.run_id,
            frame_id=worktree.frame_id,
            attempt=attempt,
            worktree=str(worktree.path),
            mechanism="setup",
            duration_s=duration_s,
            outcome="ok" if result.returncode == 0 else "failed",
            output_tail=output_tail,
        ))
        if result.returncode != 0:
            raise WorktreeProvisioningError(
                f"worktree setup failed with exit {result.returncode}\n{output_tail}"
            )

    def _provision_links(
        self, worktree: FrameWorktree, attempt: int, links: tuple[str, ...]
    ) -> None:
        if self._provisioning_recorded(worktree, "link"):
            return
        started = time.monotonic()
        linked: list[str] = []
        warnings: list[str] = []
        try:
            with self._workspace_lock:
                self.repository.append_excludes(worktree.path, list(links))
            for relative in links:
                source = self.repository.root / relative
                if not source.exists():
                    warnings.append(
                        "worktree link source does not exist; skipped: " + relative
                    )
                    continue
                destination = worktree.path / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                parent = destination.parent.resolve()
                if not parent.is_relative_to(worktree.path.resolve()):
                    raise WorktreeProvisioningError(
                        f"worktree link destination escapes checkout: {relative}"
                    )
                if destination.exists() or destination.is_symlink():
                    if (
                        destination.is_file()
                        and not destination.is_symlink()
                        and self.repository.is_tracked(worktree.path, relative)
                    ):
                        # Let the checkout-wide cleanliness assertion diagnose a
                        # configured link that shadows a tracked file.
                        destination.unlink()
                    else:
                        raise WorktreeProvisioningError(
                            f"worktree link destination already exists: {relative}"
                        )
                destination.symlink_to(source, target_is_directory=source.is_dir())
                linked.append(relative)
        except (OSError, RepositoryError, WorktreeProvisioningError) as exc:
            duration_s = max(0.0, time.monotonic() - started)
            detail = f"worktree link provisioning failed: {exc}"
            self.journal.append(WorktreeProvisioned(
                run_id=self.run_id,
                frame_id=worktree.frame_id,
                attempt=attempt,
                worktree=str(worktree.path),
                mechanism="link",
                duration_s=duration_s,
                outcome="failed",
                output_tail=detail,
                linked=linked,
                warnings=warnings,
            ))
            raise WorktreeProvisioningError(detail) from exc
        duration_s = max(0.0, time.monotonic() - started)
        self.journal.append(WorktreeProvisioned(
            run_id=self.run_id,
            frame_id=worktree.frame_id,
            attempt=attempt,
            worktree=str(worktree.path),
            mechanism="link",
            duration_s=duration_s,
            outcome="ok" if linked else "skipped",
            linked=linked,
            warnings=warnings,
        ))

    def _provision_worktree(
        self, worktree: FrameWorktree, attempt: int
    ) -> None:
        if self._worktree_setup is not None:
            self._provision_setup(worktree, attempt, self._worktree_setup)
        if self._worktree_links:
            self._provision_links(worktree, attempt, self._worktree_links)
        status = self.repository.status_porcelain(worktree.path)
        if status:
            raise WorktreeProvisioningError(
                "worktree provisioning left checkout dirty; refusing to launch "
                "the frame.\n"
                f"git status --porcelain:\n{status.rstrip()}"
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
        start_barrier: threading.Barrier | None = None,
        cancellation: threading.Event | None = None,
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
            if existing is not None and existing.outcome is None:
                self._guard_frame_relaunch(projection, existing)
            resume = existing is not None
        earlier_attempt: str | None = None
        if existing is not None:
            # A same-process replay must settle the old call frontier before
            # stale-owner cleanup can touch its worktree. Capture its surviving
            # evidence after that join, but before force-removing the stale owner.
            try:
                self._join_frame_calls(frame_id)
            except FrameCallJoinTimeoutError as exc:
                self.journal.append(FrameExited(
                    run_id=self.run_id,
                    frame_id=frame_id,
                    attempt=existing.attempt,
                    outcome="failed",
                    text=f"frame call join failed after retries: {exc}",
                    stderr=str(exc),
                    head=self.repository.branch_tip(branch),
                ))
                return self.journal.projection.frame(frame_id)
            with self._call_condition:
                self._terminating_frames.discard(frame_id)
            earlier_attempt = self._earlier_attempt_block(existing)
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
        try:
            self._provision_worktree(worktree, attempt)
        except WorktreeProvisioningError as exc:
            try:
                self.journal.append(FrameExited(
                    run_id=self.run_id,
                    frame_id=frame_id,
                    attempt=attempt,
                    outcome="failed",
                    text=str(exc),
                    stderr=str(exc),
                    head=self.repository.head(worktree.path),
                ))
                return self.journal.projection.frame(frame_id)
            finally:
                with self._workspace_lock:
                    self.repository.remove_worktree(worktree)
        with self._active_lock:
            self._active[frame_id] = worktree
        capability = self.server.register_frame(frame_id)
        terminalized = False
        worker: WorkerLease | None = None
        execution: _FrameExecution | None = None
        try:
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
                rendered_prompt = self._frame_prompt(
                    frame_id,
                    prompt,
                    skills,
                    worktree.path,
                    earlier_attempt=earlier_attempt,
                )
                worker = self._workers.prepare(
                    frame_id, attempt, runtime_dir / "worker.handle"
                )
                execution = self._register_execution(
                    frame_id, attempt, rig, worker
                )
                if self._acquire_frame_slot(execution, cancellation):
                    adapter = self.registry.resolve(rig)
                    runtime = FrameRuntime(
                        endpoint=self.server.endpoint,
                        token=capability,
                        frame_id=frame_id,
                        shim_path=shim,
                        runtime_dir=runtime_dir,
                        next_call_index=next_call_index,
                        cancellation=cancellation,
                        worker=worker,
                        environment=self._slot_environment(execution.lane),
                        backstop_timeout_s=adapter.timeout_s * 3,
                    )
                    result = adapter.run(rendered_prompt, worktree.path, runtime)
                else:
                    result = FrameResult(
                        outcome="failed",
                        text="frame execution stopped before acquiring a rig slot",
                    )
            except Exception as exc:
                if worker is not None:
                    worker.stop("rig_exception")
                result = FrameResult(
                    outcome="failed",
                    text=f"frame rig failed: {exc}",
                    stderr=str(exc),
                )
            finally:
                if execution is not None:
                    self._close_frame_execution(frame_id, "exit")
            if worker is not None:
                worker.finished()
            if execution is not None and execution.timed_out:
                result = FrameResult(
                    outcome="failed",
                    text=(
                        "[timeout] frame self-time budget "
                        f"of {execution.budget_s:g}s exhausted"
                    ),
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )

            # MCP validation transfers lifetime ownership to the engine. No frame
            # effect below may race a detached worker from this attempt. Exhausting
            # the retried join is terminal only for this call and frame.
            try:
                self._join_frame_calls(frame_id)
            except FrameCallJoinTimeoutError as exc:
                result = FrameResult(
                    outcome="failed",
                    text=f"frame call join failed after retries: {exc}",
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=str(exc),
                )
            self._raise_active_relaunch_block()
            if result.outcome == "ok":
                try:
                    committed = self.repository.auto_commit(
                        worktree.path, f"wildflows frame {frame_id}"
                    )
                    head = committed.head
                    if committed.skipped_symlinks:
                        paths = list(committed.skipped_symlinks)
                        self.journal.append(FrameCommitWarning(
                            run_id=self.run_id,
                            frame_id=frame_id,
                            attempt=attempt,
                            skipped_paths=paths,
                            message=(
                                "frame auto-commit skipped new out-of-tree "
                                f"symlinks: {', '.join(paths)}"
                            ),
                        ))
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
            if result.outcome != "ok":
                self._snapshot_failed_worktree(frame_id, attempt, worktree.path)
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
            terminalized = True
            return self.journal.projection.frame(frame_id)
        finally:
            if terminalized:
                self.server.revoke_frame(capability)
                with self._call_condition:
                    self._terminating_frames.discard(frame_id)
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
        *,
        earlier_attempt: str | None = None,
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
            "wildflows_ask. Tool calls block; successful child commits are present in "
            "your branch when dispatch returns. A failed child result includes its "
            "salvage branch, head, and diffstat; pass retry_frame alone to relaunch a "
            "failed direct child on that branch. Dispatch skills is optional and contains one "
            "ordered skill-name list per task. Dispatch rig accepts one registry key for "
            "every task or a parallel list; omission and null list entries inherit this "
            "frame's rig. Dispatch kinds is an optional parallel list describing the "
            "nature of each task; kinds are journalled hints with no routing power. "
            "Shapes are your control flow: a sequence "
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
        if earlier_attempt is not None:
            preamble += f"\n{earlier_attempt}"
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
            unsafe_symlinks = self.repository.new_out_of_tree_commit_symlinks(
                frame.base_commit, frame.head
            )
            if unsafe_symlinks:
                raise IntegrationError(
                    "refusing integration of new out-of-tree symlinks: "
                    f"{', '.join(unsafe_symlinks)}"
                )
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
        cancellation = self._current_call_cancellation()
        timeout_s = self.registry.gate_timeout(frame.rig)
        self._pause_frame_self_time(frame.frame_id)
        try:
            if cancellation.is_set():
                gate = GateResult(
                    exit_code=125,
                    stdout="",
                    stderr="[cancelled] frame call stopped before command launch",
                )
            else:
                result = run_shell(
                    request.cmd,
                    worktree.path,
                    timeout_s,
                    cancellation=cancellation,
                )
                if result.cancelled:
                    gate = GateResult(
                        exit_code=125,
                        stdout=result.stdout,
                        stderr=(
                            result.stderr
                            + "\n[cancelled] frame call execution stopped"
                        ),
                    )
                elif result.timed_out:
                    assert timeout_s is not None
                    gate = GateResult(
                        exit_code=124,
                        stdout=result.stdout,
                        stderr=(
                            result.stderr
                            + f"\n[timeout] gate exceeded {timeout_s:g}s"
                        ),
                    )
                else:
                    assert result.returncode is not None
                    gate = GateResult(
                        exit_code=result.returncode,
                        stdout=result.stdout,
                        stderr=result.stderr,
                    )
            self._append_tool_return(GateReturned(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                result=gate,
            ))
            return gate
        finally:
            self._resume_frame_self_time(frame.frame_id)

    def _ask(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: AskRequest,
        replaying: bool,
    ) -> AskResult | ToolFailure:
        caller_head = self.repository.ensure_clean(worktree.path, frame.branch)
        if not replaying:
            asked = Asked(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                request=request,
                caller_head=caller_head,
            )
            self.journal.append(asked)
            self._notify_asked(asked)
        answer_path = self._answer_path(frame.frame_id, call_index)
        cancellation = self._current_call_cancellation()
        with self._answer_condition:
            while not answer_path.is_file() and not cancellation.is_set():
                self._answer_condition.wait(timeout=0.25)
        if cancellation.is_set() and not answer_path.is_file():
            failure = ToolFailure(
                error_code="frame_terminating",
                message="owner ask stopped because its caller frame terminated",
            )
            self.journal.append(CallFailed(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                tool="ask",
                request=request,
                result=failure,
            ))
            return failure
        answer = answer_path.read_text(encoding="utf-8")
        self._append_tool_return(Answered(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            answer=answer,
        ))
        return AskResult(answer=answer)

    def _notify_asked(self, asked: Asked) -> None:
        if not self._notify_command:
            return
        environment = {
            **os.environ,
            "WILDFLOWS_QUESTION": asked.request.question,
            "WILDFLOWS_FRAME_ID": asked.frame_id,
            "WILDFLOWS_RUN_ID": self.run_id,
            "WILDFLOWS_NOTIFY_QUESTION": asked.request.question,
            "WILDFLOWS_NOTIFY_FRAME_ID": asked.frame_id,
            "WILDFLOWS_NOTIFY_RUN_ID": self.run_id,
        }
        try:
            subprocess.Popen(
                [
                    *self._notify_command,
                    asked.request.question,
                    asked.frame_id,
                    self.run_id,
                ],
                cwd=self.repository.root,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
                text=True,
                encoding="utf-8",
            )
        except (OSError, ValueError, _subprocess.SubprocessError):
            # Notification is an owner wakeup hint, never part of run correctness.
            return

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
