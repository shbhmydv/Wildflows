"""Regression coverage for joining detached MCP calls to frame teardown."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
import fcntl
from http import HTTPStatus
import http.client
import json
import os
from pathlib import Path
import threading
from urllib.parse import urlsplit

import pytest

from wildflows.engine import Engine, FrameCallJoinTimeoutError
from wildflows.events import (
    CallFailed,
    DispatchCalled,
    DispatchReturned,
    Event,
    FrameExited,
    GateCalled,
    GateReturned,
)
from wildflows.frame import (
    DispatchRequest,
    DispatchResult,
    FrameOutcome,
    FrameResult,
    FrameRuntime,
    GateRequest,
    GateResult,
    ToolFailure,
    ToolName,
)
from wildflows.mcp import FrameCallJoin, ValidatedToolCall
from wildflows.projection import FrameProjection
from wildflows.rig import RigRegistry
from wildflows.run import Run
from wildflows.workspace import FrameWorktree


@dataclass
class _DetachedToolRig:
    """Issue one valid MCP call, drop its client, and immediately return."""

    tool: ToolName
    arguments: dict[str, object]
    outcome: FrameOutcome = "ok"
    text: str = "rig returned after disconnect"
    before_return: Callable[[], None] | None = None
    timeout_s: float = 10.0
    tool_entered: threading.Event = field(default_factory=threading.Event)
    returned: threading.Event = field(default_factory=threading.Event)

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, workdir
        endpoint = urlsplit(runtime.endpoint)
        host = endpoint.hostname
        port = endpoint.port
        assert host is not None and port is not None
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "tools/call",
            "params": {
                "name": self.tool,
                "arguments": self.arguments,
                "_meta": {"wildflows": {"callIndex": 0}},
            },
        }
        connection = http.client.HTTPConnection(host, port, timeout=5)
        try:
            connection.request(
                "POST",
                endpoint.path,
                body=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {runtime.token}",
                    "Content-Type": "application/json",
                    "X-Wildflows-Frame": runtime.frame_id,
                },
            )
            response = connection.getresponse()
            assert response.status == HTTPStatus.OK
            assert response.getheader("Transfer-Encoding") == "chunked"
            assert self.tool_entered.wait(timeout=5), "validated MCP worker never entered"
        finally:
            # The worker was admitted by MCP already.  Closing only abandons the
            # response stream; it must not abandon the engine operation.
            connection.close()
        if self.before_return is not None:
            self.before_return()
        self.returned.set()
        return FrameResult(outcome=self.outcome, text=self.text, exit_code=0)


@dataclass
class _TeardownTrace:
    timeline: list[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    commit_attempted: threading.Event = field(default_factory=threading.Event)
    frame_exited: threading.Event = field(default_factory=threading.Event)

    def record(self, label: str) -> None:
        with self.lock:
            self.timeline.append(label)
        if label == "commit":
            self.commit_attempted.set()
        if label == "frame_exited":
            self.frame_exited.set()

    def snapshot(self) -> list[str]:
        with self.lock:
            return list(self.timeline)


def _engine(repo: Path, tmp_path: Path, rig: _DetachedToolRig, name: str) -> Engine:
    return Engine(
        tmp_path / f"run-{name}",
        repo,
        RigRegistry({"detached": rig}),
        run_id=f"frame-call-join-{name}",
        root_rig="detached",
        root_prompt="exercise detached tool lifetime",
        worktrees_root=tmp_path / f"worktrees-{name}",
    )


def _trace_teardown(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> _TeardownTrace:
    trace = _TeardownTrace()
    original_append = engine.journal.append
    original_commit = engine.repository.commit_all
    original_revoke = engine.server.revoke_frame
    original_remove = engine.repository.remove_worktree

    def append(event: Event) -> int:
        sequence = original_append(event)
        if isinstance(event, CallFailed):
            trace.record("call_failed")
        elif isinstance(event, GateReturned):
            trace.record("gate_returned")
        elif isinstance(event, DispatchReturned):
            trace.record("dispatch_returned")
        elif isinstance(event, FrameExited):
            trace.record("frame_exited")
        return sequence

    def commit_all(worktree: Path, message: str) -> str:
        trace.record("commit")
        return original_commit(worktree, message)

    def revoke(capability: str) -> None:
        trace.record("capability_revoked")
        original_revoke(capability)

    def remove(worktree: FrameWorktree) -> None:
        trace.record("worktree_removed")
        original_remove(worktree)

    monkeypatch.setattr(engine.journal, "append", append)
    monkeypatch.setattr(engine.repository, "commit_all", commit_all)
    monkeypatch.setattr(engine.server, "revoke_frame", revoke)
    monkeypatch.setattr(engine.repository, "remove_worktree", remove)
    return trace


def _start(engine: Engine) -> tuple[threading.Thread, list[FrameResult], list[BaseException]]:
    results: list[FrameResult] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            results.append(engine.run())
        except BaseException as exc:  # surfaced after deterministic cleanup below
            errors.append(exc)

    thread = threading.Thread(target=run, name="frame-call-join-test")
    thread.start()
    return thread, results, errors


def _assert_joined_teardown(
    trace: _TeardownTrace, return_label: str, *, committed: bool
) -> None:
    timeline = trace.snapshot()
    assert return_label in timeline
    required = ["frame_exited", "capability_revoked", "worktree_removed"]
    if committed:
        required.insert(0, "commit")
    for label in required:
        assert label in timeline
        assert timeline.index(return_label) < timeline.index(label)


def test_disconnected_validated_call_joins_before_commit_revocation_and_removal(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A disconnected call already executing in Engine must delay successful exit."""
    rig = _DetachedToolRig("gate", {"cmd": "printf joined"})
    engine = _engine(repo, tmp_path, rig, "success")
    trace = _trace_teardown(engine, monkeypatch)
    release_gate = threading.Event()
    gate_finished = threading.Event()
    original_gate = engine._gate  # noqa: SLF001 - precise frame-call join seam

    def blocked_gate(
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: GateRequest,
        replaying: bool,
    ) -> GateResult:
        rig.tool_entered.set()
        try:
            assert release_gate.wait(timeout=5), "test did not release detached gate"
            return original_gate(frame, worktree, call_index, digest, request, replaying)
        finally:
            gate_finished.set()

    monkeypatch.setattr(engine, "_gate", blocked_gate)
    thread, results, errors = _start(engine)
    committed_while_call_live = False
    try:
        assert rig.returned.wait(timeout=5), "rig did not reach its terminal return"
        committed_while_call_live = trace.commit_attempted.wait(timeout=0.5)
    finally:
        release_gate.set()
        assert gate_finished.wait(timeout=5), "detached gate did not finish"
        thread.join(timeout=10)

    assert not thread.is_alive()
    assert not errors
    assert results and results[0].outcome == "ok"
    assert not committed_while_call_live

    events = engine.journal.events()
    called = [event for event in events if isinstance(event, GateCalled)]
    returned = [event for event in events if isinstance(event, GateReturned)]
    exited = [event for event in events if isinstance(event, FrameExited)]
    assert len(called) == len(returned) == len(exited) == 1
    assert returned[0].seq < exited[0].seq
    assert returned[0].result == GateResult(exit_code=0, stdout="joined", stderr="")
    call = engine.projection.call("f0", 0)
    assert call is not None and call.completed
    assert call.finished_seq < engine.projection.frame("f0").exited_seq
    _assert_joined_teardown(trace, "gate_returned", committed=True)


def test_stopped_worker_without_a_durable_return_gets_typed_failed_result(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A reaped worker-side execution cannot leave frame exit waiting forever."""
    rig = _DetachedToolRig("gate", {"cmd": "never executed"})
    engine = _engine(repo, tmp_path, rig, "stopped-without-return")
    trace = _trace_teardown(engine, monkeypatch)

    def stopped_without_return(
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: GateRequest,
        replaying: bool,
    ) -> GateResult:
        del frame, worktree, call_index, digest, request, replaying
        rig.tool_entered.set()
        raise RuntimeError("execution was reaped before its return append")

    monkeypatch.setattr(engine, "_gate", stopped_without_return)
    result = engine.run()

    assert result.outcome == "ok"
    failures = [
        event for event in engine.journal.events() if isinstance(event, CallFailed)
    ]
    exited = [
        event for event in engine.journal.events() if isinstance(event, FrameExited)
    ]
    assert len(failures) == len(exited) == 1
    failure = failures[0]
    assert failure.seq < exited[0].seq
    assert failure.result == ToolFailure(
        error_code="worker_stopped_without_return",
        message=(
            "validated MCP worker stopped without a durable tool-specific return"
        ),
    )
    call = engine.projection.call("f0", 0)
    assert call is not None and call.completed
    assert call.response == failure.result
    _assert_joined_teardown(trace, "call_failed", committed=True)


def test_terminal_attempt_cannot_journal_a_typed_worker_refusal_after_frame_exit(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A live worker's typed refusal is durable before exit, never after it."""
    rig = _DetachedToolRig(
        "dispatch",
        {"tasks": ["unused"], "rig": "detached", "parallel": False},
    )
    engine = _engine(repo, tmp_path, rig, "typed-refusal")
    trace = _trace_teardown(engine, monkeypatch)
    release_worker = threading.Event()
    worker_finished = threading.Event()

    def delayed_refusal(
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: DispatchRequest,
        replaying: bool,
    ) -> DispatchResult:
        assert not replaying
        caller_head = engine.repository.ensure_clean(worktree.path, frame.branch)
        engine.journal.append(DispatchCalled(
            run_id=engine.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            request=request,
            caller_head=caller_head,
        ))
        rig.tool_entered.set()
        try:
            assert release_worker.wait(timeout=5), "test did not release detached worker"
            refusal = DispatchResult(
                outcome="refused",
                error_code="frame_terminating",
                message="frame terminal request refused the live dispatch",
            )
            engine.journal.append(DispatchReturned(
                run_id=engine.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                result=refusal,
            ))
            return refusal
        finally:
            worker_finished.set()

    monkeypatch.setattr(engine, "_dispatch", delayed_refusal)
    thread, results, errors = _start(engine)
    exited_while_worker_live = False
    try:
        assert rig.returned.wait(timeout=5), "rig did not reach its terminal return"
        exited_while_worker_live = trace.frame_exited.wait(timeout=0.5)
    finally:
        release_worker.set()
        assert worker_finished.wait(timeout=5), "detached worker did not finish"
        thread.join(timeout=10)

    assert not thread.is_alive()
    assert not errors
    assert results and results[0].outcome == "ok"
    assert not exited_while_worker_live

    events = engine.journal.events()
    returned = [event for event in events if isinstance(event, DispatchReturned)]
    exited = [event for event in events if isinstance(event, FrameExited)]
    assert len(returned) == len(exited) == 1
    assert returned[0].result.outcome == "refused"
    assert returned[0].result.error_code == "frame_terminating"
    assert returned[0].seq < exited[0].seq
    call = engine.projection.call("f0", 0)
    assert call is not None and call.completed
    assert call.finished_seq < engine.projection.frame("f0").exited_seq
    _assert_joined_teardown(trace, "dispatch_returned", committed=True)


def test_run_does_not_retain_lifecycle_for_an_escaped_join_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The obsolete fatal-timeout special case cannot strand lifecycle ownership."""
    run = object.__new__(Run)
    descriptor = os.open(tmp_path / "run.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    run._lifecycle_descriptor = descriptor  # noqa: SLF001 - lifecycle failure seam

    def fail_join() -> object:
        raise FrameCallJoinTimeoutError("worker did not confirm stop")

    monkeypatch.setattr(run, "_drive", fail_join)
    with pytest.raises(FrameCallJoinTimeoutError, match="confirm stop"):
        run.run()
    assert run._lifecycle_descriptor is None  # noqa: SLF001


def test_join_timeout_retries_then_fails_only_call_and_frame(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exhausting the bookkeeping join is a durable frame failure, not a run crash."""
    rig = _DetachedToolRig("gate", {"cmd": "printf never"})
    engine = _engine(repo, tmp_path, rig, "timeout-isolated")
    request = GateRequest(cmd="printf never")
    active = ValidatedToolCall("f0", 0, "gate", request, 0)
    waits: list[float] = []

    def never_joins(frame_id: str, timeout: float) -> FrameCallJoin:
        assert frame_id == "f0"
        waits.append(timeout)
        return FrameCallJoin(completed=(), active=(active,))

    def issue_without_worker(
        prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, workdir, runtime
        return FrameResult(outcome="ok", text="adapter exited")

    monkeypatch.setattr(rig, "run", issue_without_worker)
    monkeypatch.setattr(engine.server, "join_frame", never_joins)
    monkeypatch.setattr("wildflows.engine.FRAME_CALL_JOIN_TIMEOUT_S", 0.01)
    monkeypatch.setattr(
        "wildflows.engine.FRAME_CALL_JOIN_RETRY_BACKOFF_S", (0.01, 0.02)
    )

    result = engine.run()

    assert result.outcome == "failed"
    assert len(waits) == 4  # natural grace, cancellation grace, two retries
    assert waits[-2:] == [0.01, 0.02]
    failures = [
        event for event in engine.journal.events() if isinstance(event, CallFailed)
    ]
    exited = [
        event for event in engine.journal.events() if isinstance(event, FrameExited)
    ]
    assert len(failures) == len(exited) == 1
    assert failures[0].result.error_code == "frame_call_join_timeout"
    assert failures[0].seq < exited[0].seq
    assert exited[0].outcome == "failed"
    assert engine.projection.finished is not None
    assert engine.projection.finished.outcome == "failed"


def test_cancellation_record_without_confirmed_stop_does_not_close_call_join(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A terminal cancellation report is not a join acknowledgement from its worker."""
    cancellation_recorded = threading.Event()
    cancellation_observed = threading.Event()
    release_execution = threading.Event()
    execution_stopped = threading.Event()
    rig = _DetachedToolRig(
        "dispatch",
        {"tasks": ["unused"], "rig": "detached", "parallel": False},
        outcome="failed",
        text="cancellation recorded without execution-stop confirmation",
        before_return=cancellation_recorded.set,
    )
    engine = _engine(repo, tmp_path, rig, "cancellation")
    trace = _trace_teardown(engine, monkeypatch)
    cancellation_requested = threading.Event()
    original_cancel = engine._cancel_frame_calls  # noqa: SLF001 - join protocol seam

    def observe_cancel(frame_id: str) -> None:
        original_cancel(frame_id)
        cancellation_requested.set()

    monkeypatch.setattr("wildflows.engine.FRAME_CALL_JOIN_TIMEOUT_S", 2.0)
    monkeypatch.setattr(engine, "_cancel_frame_calls", observe_cancel)

    def cancellation_ignoring_dispatch(
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: DispatchRequest,
        replaying: bool,
    ) -> DispatchResult:
        assert not replaying
        caller_head = engine.repository.ensure_clean(worktree.path, frame.branch)
        engine.journal.append(DispatchCalled(
            run_id=engine.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            request=request,
            caller_head=caller_head,
        ))
        rig.tool_entered.set()
        assert cancellation_recorded.wait(timeout=5), "rig never recorded cancellation"
        cancellation_observed.set()
        try:
            # Observing/recording cancellation is deliberately insufficient: this
            # worker continues until execution is actually stopped below.
            assert release_execution.wait(timeout=5), "test did not stop execution"
            execution_stopped.set()
            refusal = DispatchResult(
                outcome="refused",
                error_code="cancelled",
                message="execution stopped after cancellation confirmation",
            )
            engine.journal.append(DispatchReturned(
                run_id=engine.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                result=refusal,
            ))
            return refusal
        finally:
            execution_stopped.set()

    monkeypatch.setattr(engine, "_dispatch", cancellation_ignoring_dispatch)
    thread, results, errors = _start(engine)
    exited_after_record_only = False
    try:
        assert rig.returned.wait(timeout=5), "rig did not record its cancellation return"
        assert cancellation_observed.wait(timeout=5), "worker did not observe cancellation"
        assert cancellation_requested.wait(timeout=1.5), "engine did not request cancellation"
        assert not execution_stopped.is_set()
        exited_after_record_only = trace.frame_exited.wait(timeout=0.2)
    finally:
        release_execution.set()
        assert execution_stopped.wait(timeout=5), "worker did not confirm execution stop"
        thread.join(timeout=10)

    assert not thread.is_alive()
    assert not errors
    assert results and results[0].outcome == "failed"
    assert not exited_after_record_only

    events = engine.journal.events()
    returned = [event for event in events if isinstance(event, DispatchReturned)]
    exited = [event for event in events if isinstance(event, FrameExited)]
    assert len(returned) == len(exited) == 1
    assert returned[0].result.error_code == "cancelled"
    assert returned[0].seq < exited[0].seq
    assert exited[0].outcome == "failed"
    call = engine.projection.call("f0", 0)
    assert call is not None and call.completed
    assert call.finished_seq < engine.projection.frame("f0").exited_seq
    _assert_joined_teardown(trace, "dispatch_returned", committed=False)
    assert "commit" not in trace.snapshot()
