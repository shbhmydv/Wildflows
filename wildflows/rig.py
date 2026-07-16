"""Frame rig adapters: prompt + CWD + engine capability -> resident agent result."""
from __future__ import annotations

import ctypes
import json
import os
import re
import shlex
import signal
import subprocess
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, TextIO, runtime_checkable

from wildflows.frame import FrameOutcome, FrameResult, FrameRuntime, WorkerLease

__all__ = [
    "DEFAULT_BUSY_PATTERNS",
    "ExternalResult",
    "WorkerReap",
    "WorkerSupervisor",
    "run_shell",
    "Rig",
    "EchoRig",
    "ShellRig",
    "ScriptRig",
    "RigRegistry",
]

_PRCTL: Callable[[int, int, int, int, int], int] | None = None
if os.name == "posix" and Path("/proc").is_dir():
    _libc = ctypes.CDLL(None, use_errno=True)
    _raw_prctl = _libc.prctl
    _raw_prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    _raw_prctl.restype = ctypes.c_int
    _PRCTL = _raw_prctl


def _die_with_parent(parent_pid: int) -> None:
    if _PRCTL is None or _PRCTL(1, signal.SIGKILL, 0, 0, 0) != 0:
        os._exit(127)
    if os.getppid() != parent_pid:
        os._exit(127)


def _guarded_child(parent_pid: int) -> None:
    """Fence the workload and kill its whole process group after parent death."""
    _die_with_parent(parent_pid)
    leader_pid = os.getpid()
    watchdog_pid = os.fork()
    if watchdog_pid != 0:
        return
    # Popen waits for its exec-error pipe to close. The watchdog never execs,
    # so it must drop every inherited descriptor or parent launch deadlocks.
    for name in os.listdir("/proc/self/fd"):
        descriptor = int(name)
        if descriptor > 2:
            try:
                os.close(descriptor)
            except OSError:
                pass
    while os.getppid() == leader_pid:
        time.sleep(0.02)
    try:
        os.killpg(leader_pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    os._exit(0)


DEFAULT_BUSY_PATTERNS = [
    r"rate.?limit",
    r"429",
    r"quota",
    r"usage limit",
    r"session limit",
    r"weekly limit",
    r"plan limit",
    r"too many requests",
]


@dataclass(frozen=True)
class ExternalResult:
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool = False
    cancelled: bool = False


@dataclass(frozen=True)
class _WorkerProcess:
    pid: int
    process_group_id: int
    session_id: int
    start_time: int | None = None


@dataclass(frozen=True)
class WorkerReap:
    """A signalled adapter session and the durable identity that owned it."""

    frame_id: str
    attempt: int
    pid: int
    process_group_id: int
    session_id: int
    reason: str
    escalated: bool


def _process_identity(pid: int) -> tuple[str, int, int, int] | None:
    try:
        raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    closing = raw.rfind(")")
    if closing < 0:
        return None
    fields = raw[closing + 2:].split()
    if len(fields) < 20:
        return None
    try:
        return fields[0], int(fields[2]), int(fields[3]), int(fields[19])
    except ValueError:
        return None


def _session_members(session_id: int) -> set[int]:
    members: set[int] = set()
    try:
        entries = Path("/proc").iterdir()
    except FileNotFoundError:
        return members
    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        identity = _process_identity(pid)
        if identity is not None and identity[0] != "Z" and identity[2] == session_id:
            members.add(pid)
    return members


def _signal_process_tree(record: _WorkerProcess, sig: signal.Signals) -> bool:
    own_session = os.getsid(0)
    if record.session_id <= 1 or record.session_id == own_session:
        raise RuntimeError(
            f"refusing to signal unsafe worker session {record.session_id}"
        )
    members = _session_members(record.session_id)
    if not members:
        return False
    leader = _process_identity(record.pid)
    if (
        record.start_time is not None
        and leader is not None
        and leader[3] != record.start_time
    ):
        return False
    sent = False
    if any(
        (identity := _process_identity(pid)) is not None
        and identity[1] == record.process_group_id
        for pid in members
    ):
        try:
            os.killpg(record.process_group_id, sig)
            sent = True
        except ProcessLookupError:
            pass
    for pid in members:
        if pid == os.getpid():
            continue
        try:
            os.kill(pid, sig)
            sent = True
        except ProcessLookupError:
            pass
    return sent


@contextmanager
def _defer_shutdown_signals() -> Iterator[None]:
    if (
        threading.current_thread() is not threading.main_thread()
        or not hasattr(signal, "pthread_sigmask")
    ):
        yield
        return
    watched = {signal.SIGINT, signal.SIGTERM}
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, watched)
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def _wait_for_session_exit(session_id: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while _session_members(session_id):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.02, remaining))
    return True


def _reap_process_tree(
    record: _WorkerProcess, *, grace_s: float
) -> tuple[bool, bool]:
    term_sent = _signal_process_tree(record, signal.SIGTERM)
    if not term_sent:
        return False, False
    if _wait_for_session_exit(record.session_id, grace_s):
        return True, False
    escalated = _signal_process_tree(record, signal.SIGKILL)
    if not _wait_for_session_exit(record.session_id, 1.0):
        # Repeat once for descendants forked during the first session snapshot.
        _signal_process_tree(record, signal.SIGKILL)
        if not _wait_for_session_exit(record.session_id, 1.0):
            raise RuntimeError(
                f"worker session {record.session_id} survived SIGKILL"
            )
    return True, escalated


def _write_worker_handle(path: Path, record: _WorkerProcess) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    data = json.dumps({
        "version": 2,
        "pid": record.pid,
        "process_group_id": record.process_group_id,
        "session_id": record.session_id,
        "start_time": record.start_time,
    }, sort_keys=True)
    with open(temporary, "w", encoding="utf-8") as stream:
        stream.write(data + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _read_worker_handle(path: Path) -> _WorkerProcess | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    if not raw:
        return None
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        # v1 adapters wrote one PGID. Their launch group was also their session
        # leader, so treating that value as pid/pgid/sid preserves old handles.
        try:
            legacy = int(raw)
        except ValueError:
            return None
        return _WorkerProcess(legacy, legacy, legacy)
    if type(decoded) is int and decoded > 0:
        return _WorkerProcess(decoded, decoded, decoded)
    if not isinstance(decoded, dict):
        return None
    pid = decoded.get("pid")
    process_group_id = decoded.get("process_group_id")
    session_id = decoded.get("session_id")
    start_time = decoded.get("start_time")
    if not all(type(value) is int and value > 0 for value in (
        pid, process_group_id, session_id
    )):
        return None
    assert isinstance(pid, int)
    assert isinstance(process_group_id, int)
    assert isinstance(session_id, int)
    if start_time is not None and (type(start_time) is not int or start_time < 0):
        return None
    assert isinstance(start_time, int) or start_time is None
    return _WorkerProcess(pid, process_group_id, session_id, start_time)


class _SupervisorLease:
    def __init__(
        self,
        supervisor: "WorkerSupervisor",
        frame_id: str,
        attempt: int,
        handle_path: Path,
    ) -> None:
        self._supervisor = supervisor
        self.frame_id = frame_id
        self.attempt = attempt
        self.handle_path = handle_path
        self.record: _WorkerProcess | None = None
        self.stop_reason: str | None = None

    def started(
        self,
        pid: int,
        process_group_id: int,
        session_id: int,
        start_time: int | None = None,
    ) -> None:
        self._supervisor._started(
            self, _WorkerProcess(pid, process_group_id, session_id, start_time)
        )

    def stop(self, reason: str) -> None:
        self._supervisor._stop(self, reason)

    def finished(self) -> None:
        self._supervisor._finished(self)


class WorkerSupervisor:
    """Engine-owned registry and synchronous reaper for rig adapter sessions."""

    def __init__(
        self,
        on_reaped: Callable[[WorkerReap], None],
        *,
        grace_s: float = 0.25,
    ) -> None:
        self._on_reaped = on_reaped
        self._grace_s = grace_s
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._leases: dict[tuple[str, int], _SupervisorLease] = {}
        self._reaping = 0
        self._shutdown_reason: str | None = None

    def prepare(
        self, frame_id: str, attempt: int, handle_path: Path
    ) -> WorkerLease:
        key = (frame_id, attempt)
        with self._lock:
            if self._shutdown_reason is not None:
                raise RuntimeError("worker supervisor is already shut down")
            if key in self._leases:
                raise RuntimeError(f"worker handle already active for {frame_id}:{attempt}")
            lease = _SupervisorLease(self, frame_id, attempt, handle_path)
            self._leases[key] = lease
            return lease

    def adopt_and_reap(
        self, frame_id: str, attempt: int, handle_path: Path, reason: str
    ) -> None:
        lease = self.prepare(frame_id, attempt, handle_path)
        lease.stop(reason)

    def _started(self, lease: _SupervisorLease, record: _WorkerProcess) -> None:
        if record.pid <= 0 or record.process_group_id <= 0 or record.session_id <= 0:
            raise ValueError("worker process identity must be positive")
        # Publish the live identity in memory before durable I/O: a handle-write
        # exception must still leave shutdown enough authority to reap the child.
        with self._lock:
            lease.record = record
            reason = lease.stop_reason
        _write_worker_handle(lease.handle_path, record)
        if reason is not None:
            self._stop(lease, reason)

    def _record(self, lease: _SupervisorLease) -> _WorkerProcess | None:
        return lease.record or _read_worker_handle(lease.handle_path)

    def _stop(self, lease: _SupervisorLease, reason: str) -> None:
        with _defer_shutdown_signals():
            key = (lease.frame_id, lease.attempt)
            with self._lock:
                if key not in self._leases:
                    return
                lease.stop_reason = reason
                record = self._record(lease)
                if record is None:
                    return
                self._leases.pop(key, None)
                self._reaping += 1
            self._reap(lease, record, reason)

    def _finished(self, lease: _SupervisorLease) -> None:
        with _defer_shutdown_signals():
            key = (lease.frame_id, lease.attempt)
            with self._lock:
                if key not in self._leases:
                    return
                record = self._record(lease)
                self._leases.pop(key, None)
                if record is not None:
                    self._reaping += 1
            if record is not None:
                self._reap(lease, record, "worker_exit_cleanup")

    def _reap(
        self, lease: _SupervisorLease, record: _WorkerProcess, reason: str
    ) -> None:
        try:
            signalled, escalated = _reap_process_tree(
                record, grace_s=self._grace_s
            )
            if signalled:
                self._on_reaped(WorkerReap(
                    frame_id=lease.frame_id,
                    attempt=lease.attempt,
                    pid=record.pid,
                    process_group_id=record.process_group_id,
                    session_id=record.session_id,
                    reason=reason,
                    escalated=escalated,
                ))
        finally:
            with self._condition:
                self._reaping -= 1
                self._condition.notify_all()

    @staticmethod
    def _stop_all(leases: list[_SupervisorLease], reason: str) -> None:
        first_error: BaseException | None = None
        for lease in leases:
            try:
                lease.stop(reason)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise RuntimeError("one or more worker sessions could not be reaped") from first_error

    def reap_frames(self, frame_ids: set[str], reason: str) -> None:
        with _defer_shutdown_signals():
            with self._lock:
                leases = [
                    lease for lease in self._leases.values()
                    if lease.frame_id in frame_ids
                ]
            self._stop_all(leases, reason)

    def shutdown(self, reason: str) -> None:
        with _defer_shutdown_signals():
            with self._lock:
                if self._shutdown_reason is None:
                    self._shutdown_reason = reason
                leases = list(self._leases.values())
            try:
                self._stop_all(leases, reason)
            finally:
                with self._condition:
                    while self._reaping:
                        self._condition.wait()


def _kill_unmanaged_process(process: subprocess.Popen[str]) -> None:
    record = _WorkerProcess(process.pid, process.pid, process.pid)
    _reap_process_tree(record, grace_s=0.25)
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _capture(
    command: str | list[str],
    *,
    cwd: Path,
    timeout_s: float | None,
    shell: bool,
    env: dict[str, str] | None = None,
    cancellation: threading.Event | None = None,
    worker: WorkerLease | None = None,
) -> ExternalResult:
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stdout, \
            tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr:
        out_file: TextIO = stdout
        err_file: TextIO = stderr
        parent_pid = os.getpid()
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=out_file,
            stderr=err_file,
            text=True,
            shell=shell,
            env=env,
            start_new_session=True,
            preexec_fn=(lambda: _guarded_child(parent_pid)) if _PRCTL else None,
        )
        identity = _process_identity(process.pid)
        if identity is None:
            process_group_id = process.pid
            session_id = process.pid
            start_time = None
        else:
            _, process_group_id, session_id, start_time = identity
        if worker is not None:
            worker.started(
                process.pid, process_group_id, session_id, start_time
            )
        timed_out = False
        cancelled = False
        returncode: int | None = None
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        try:
            while returncode is None:
                if cancellation is not None and cancellation.is_set():
                    cancelled = True
                    break
                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if remaining is not None and remaining <= 0:
                    timed_out = True
                    break
                wait_s = 0.05 if remaining is None else min(0.05, remaining)
                try:
                    returncode = process.wait(timeout=wait_s)
                except subprocess.TimeoutExpired:
                    continue
        except BaseException:
            if worker is None:
                _kill_unmanaged_process(process)
            raise
        if returncode is None:
            reason = "worker_cancelled" if cancelled else "worker_timeout"
            if worker is None:
                _kill_unmanaged_process(process)
            else:
                worker.stop(reason)
            try:
                returncode = process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                returncode = process.wait()
        elif worker is not None:
            worker.finished()
        else:
            _kill_unmanaged_process(process)
        stdout.seek(0)
        stderr.seek(0)
        return ExternalResult(
            returncode, stdout.read(), stderr.read(), timed_out, cancelled
        )


def run_shell(
    command: str,
    workdir: Path,
    timeout_s: float | None,
    env: dict[str, str] | None = None,
    cancellation: threading.Event | None = None,
    worker: WorkerLease | None = None,
) -> ExternalResult:
    return _capture(
        command,
        cwd=workdir,
        timeout_s=timeout_s,
        shell=True,
        env=env,
        cancellation=cancellation,
        worker=worker,
    )


def _runtime_env(runtime: FrameRuntime) -> dict[str, str]:
    values = {
        "WILDFLOWS_MCP_URL": runtime.endpoint,
        "WILDFLOWS_RUN_TOKEN": runtime.token,
        "WILDFLOWS_FRAME_ID": runtime.frame_id,
        "WILDFLOWS_PI_EXTENSION": str(runtime.shim_path),
        "WILDFLOWS_NEXT_CALL_INDEX": str(runtime.next_call_index),
    }
    values.update(runtime.environment or {})
    return values


@runtime_checkable
class Rig(Protocol):
    timeout_s: float

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult: ...


class EchoRig:
    timeout_s = 30.0

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del workdir, runtime
        text = f"echo: {prompt}"
        return FrameResult(text=text, exit_code=0, stdout=text)


class ShellRig:
    def __init__(self, template: str, timeout_s: float) -> None:
        self.template = template
        self.timeout_s = timeout_s

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        command = self.template.replace("{prompt}", shlex.quote(prompt))
        backstop = runtime.backstop_timeout_s or self.timeout_s
        result = run_shell(
            command,
            workdir,
            backstop,
            env={**os.environ, **_runtime_env(runtime)},
            cancellation=runtime.cancellation,
            worker=runtime.worker,
        )
        if result.cancelled:
            return FrameResult(
                text="[cancelled] frame execution stopped before teardown",
                outcome="failed",
                stdout=result.stdout,
                stderr=result.stderr,
            )
        if result.timed_out:
            return FrameResult(
                text=f"[timeout] command exceeded {backstop}s\n{result.stderr}",
                outcome="failed",
                stdout=result.stdout,
                stderr=result.stderr,
            )
        assert result.returncode is not None
        outcome: FrameOutcome = "ok" if result.returncode == 0 else "failed"
        text = result.stdout if result.returncode == 0 else (result.stderr or result.stdout)
        return FrameResult(
            text=text,
            exit_code=result.returncode,
            outcome=outcome,
            stdout=result.stdout,
            stderr=result.stderr,
        )


class ScriptRig:
    """Drive the grindstone-compatible script contract as a v2 frame."""

    def __init__(
        self,
        script: Path,
        log_dir: Path,
        timeout_s: float = 900.0,
        env: dict[str, str] | None = None,
        busy_patterns: list[str] | None = None,
    ) -> None:
        self.script = Path(script).resolve()
        self.log_dir = Path(log_dir).resolve()
        self.timeout_s = timeout_s
        self.env = dict(env or {})
        self._busy_re = re.compile(
            "|".join(busy_patterns or DEFAULT_BUSY_PATTERNS), re.IGNORECASE
        )

    def _classify(
        self, returncode: int, stdout: str, stderr: str
    ) -> FrameOutcome:
        if returncode == 0:
            return "ok"
        if self._busy_re.search(stderr) or self._busy_re.search(stdout):
            return "busy"
        return "failed"

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        suffix = 0
        while True:
            name = workdir.name if suffix == 0 else f"{workdir.name}-{suffix}"
            dispatch_dir = self.log_dir / name
            try:
                dispatch_dir.mkdir()
                break
            except FileExistsError:
                suffix += 1
        prompt_file = dispatch_dir / "prompt.txt"
        prompt_file.write_text(prompt, encoding="utf-8")
        handle_out = (
            dispatch_dir / "handle"
            if runtime.worker is None
            else runtime.worker.handle_path
        )
        backstop = runtime.backstop_timeout_s or self.timeout_s
        argv = [
            str(self.script),
            "--worktree",
            str(workdir),
            "--prompt",
            str(prompt_file),
            "--log-dir",
            str(dispatch_dir),
            "--handle-out",
            str(handle_out),
            "--timeout",
            str(backstop),
        ]
        result = _capture(
            argv,
            cwd=workdir,
            timeout_s=backstop,
            shell=False,
            env={**os.environ, **self.env, **_runtime_env(runtime)},
            cancellation=runtime.cancellation,
            worker=runtime.worker,
        )
        (dispatch_dir / "agent.stdout.log").write_text(result.stdout, encoding="utf-8")
        (dispatch_dir / "agent.stderr.log").write_text(result.stderr, encoding="utf-8")
        if result.cancelled:
            return FrameResult(
                text="[cancelled] frame execution stopped before teardown",
                outcome="failed",
                stdout=result.stdout,
                stderr=result.stderr,
            )
        if result.timed_out:
            return FrameResult(
                text=f"[timeout] script exceeded {backstop}s\n{result.stderr}",
                outcome="failed",
                stdout=result.stdout,
                stderr=result.stderr,
            )
        assert result.returncode is not None
        outcome = self._classify(result.returncode, result.stdout, result.stderr)
        text = result.stdout if result.returncode == 0 else (result.stderr or result.stdout)
        return FrameResult(
            text=text,
            exit_code=result.returncode,
            outcome=outcome,
            stdout=result.stdout,
            stderr=result.stderr,
        )


class RigRegistry:
    def __init__(
        self,
        rigs: dict[str, Rig],
        descriptions: dict[str, str] | None = None,
        *,
        slots: dict[str, int] | None = None,
        kinds: dict[str, str] | None = None,
        gate_timeouts: dict[str, float] | None = None,
    ) -> None:
        self._rigs = dict(rigs)
        supplied = descriptions or {}
        unknown = supplied.keys() - self._rigs.keys()
        if unknown:
            raise ValueError(
                f"descriptions name unknown rigs: {', '.join(sorted(unknown))}"
            )
        configured_slots = slots or {}
        unknown_slots = configured_slots.keys() - self._rigs.keys()
        if unknown_slots:
            raise ValueError(
                f"slots name unknown rigs: {', '.join(sorted(unknown_slots))}"
            )
        if any(type(value) is not int or value <= 0 for value in configured_slots.values()):
            raise ValueError("rig slots must be positive integers")
        configured_gate_timeouts = gate_timeouts or {}
        unknown_gate_timeouts = configured_gate_timeouts.keys() - self._rigs.keys()
        if unknown_gate_timeouts:
            raise ValueError(
                "gate timeouts name unknown rigs: "
                f"{', '.join(sorted(unknown_gate_timeouts))}"
            )
        if any(value <= 0 for value in configured_gate_timeouts.values()):
            raise ValueError("gate timeouts must be positive")
        configured_kinds = kinds or {}
        unknown_kind_rigs = set(configured_kinds.values()) - self._rigs.keys()
        if unknown_kind_rigs:
            raise ValueError(
                "kinds map to unknown rigs: "
                f"{', '.join(sorted(unknown_kind_rigs))}"
            )
        if any(not kind.strip() or not rig.strip() for kind, rig in configured_kinds.items()):
            raise ValueError("kind mappings must use non-blank names")
        self._slots = dict(configured_slots)
        self._kinds = dict(configured_kinds)
        self._gate_timeouts = dict(configured_gate_timeouts)
        self._descriptions: dict[str, str] = {}
        for name, description in supplied.items():
            normalized = description.strip()
            if not normalized or "\n" in normalized or "\r" in normalized:
                raise ValueError("rig descriptions must be non-blank single lines")
            self._descriptions[name] = normalized

    def __contains__(self, name: object) -> bool:
        return name in self._rigs

    def resolve(self, name: str) -> Rig:
        if name not in self._rigs:
            raise KeyError(f"unknown rig: {name!r}")
        return self._rigs[name]

    def description(self, name: str) -> str | None:
        if name not in self._rigs:
            raise KeyError(f"unknown rig: {name!r}")
        return self._descriptions.get(name)

    def slots(self, name: str) -> int | None:
        if name not in self._rigs:
            raise KeyError(f"unknown rig: {name!r}")
        return self._slots.get(name)

    def default_rig(self, kind: str) -> str | None:
        return self._kinds.get(kind)

    def gate_timeout(self, name: str) -> float | None:
        if name not in self._rigs:
            raise KeyError(f"unknown rig: {name!r}")
        return self._gate_timeouts.get(name)

    def task_rigs(
        self,
        explicit: str | None,
        kinds: list[str],
        task_count: int,
    ) -> tuple[str, ...]:
        if explicit is not None:
            return (explicit,) * task_count
        if len(kinds) != task_count:
            raise KeyError(
                "dispatch without rig requires one mapped kind per task"
            )
        resolved: list[str] = []
        for kind in kinds:
            rig = self.default_rig(kind)
            if rig is None:
                raise KeyError(f"dispatch kind {kind!r} has no default rig")
            resolved.append(rig)
        return tuple(resolved)

    @property
    def slot_capacities(self) -> dict[str, int]:
        return dict(self._slots)

    @property
    def ordered_names(self) -> tuple[str, ...]:
        """Registry keys in operator-authored YAML/mapping order."""
        return tuple(self._rigs)

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._rigs)
