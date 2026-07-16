"""Regression coverage for engine-owned worker process-tree reaping."""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from tests.conftest import executable
from wildflows.engine import Engine
from wildflows.events import WorkerReaped
from wildflows.frame import FrameResult, FrameRuntime
from wildflows.rig import RigRegistry


class _FatalEngineError(BaseException):
    pass


def _process_is_running(pid: int) -> bool:
    stat = Path(f"/proc/{pid}/stat")
    try:
        fields = stat.read_text(encoding="utf-8").split()
    except FileNotFoundError:
        return False
    return len(fields) > 2 and fields[2] != "Z"


def _wait_for_path(path: Path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {path}")


def _wait_stopped(pid: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_is_running(pid):
            return
        time.sleep(0.01)
    raise AssertionError(f"process {pid} survived engine shutdown")


def _journal_records(repo: Path, run_id: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    journal = repo / ".wildflows" / "runs" / run_id / "events.ndjson"
    for line in journal.read_text(encoding="utf-8").splitlines():
        decoded = json.loads(line)
        assert isinstance(decoded, dict)
        records.append(decoded)
    return records


@pytest.mark.parametrize("engine_signal", [signal.SIGINT, signal.SIGTERM])
def test_engine_signal_reaps_worker_session_before_exit(
    repo: Path, tmp_path: Path, engine_signal: signal.Signals
) -> None:
    """A signal cannot orphan a worker that moved a child to another PGID."""
    adapter_pid = tmp_path / f"adapter-{engine_signal.name}.pid"
    escaped_pid = tmp_path / f"escaped-{engine_signal.name}.pid"
    adapter = executable(
        tmp_path / f"adapter-{engine_signal.name}",
        f"""#!/usr/bin/env python3
import os
from pathlib import Path
import signal
import time

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path({str(adapter_pid)!r}).write_text(str(os.getpid()), encoding="utf-8")
child = os.fork()
if child == 0:
    os.setpgid(0, 0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path({str(escaped_pid)!r}).write_text(str(os.getpid()), encoding="utf-8")
    while True:
        time.sleep(1)
while True:
    time.sleep(1)
""",
    )
    job = tmp_path / "job.md"
    job.write_text("wait for the shutdown regression", encoding="utf-8")
    rigs = tmp_path / "rigs.yaml"
    rigs.write_text(
        f"""rigs:
  sleeper:
    kind: script
    script: {adapter}
    log_dir: {tmp_path / 'logs'}
    timeout_s: 60
""",
        encoding="utf-8",
    )
    run_id = f"signal-reap-{engine_signal.name.lower()}"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "wildflows",
            "run",
            str(job),
            "--repo",
            str(repo),
            "--rigs",
            str(rigs),
            "--root-rig",
            "sleeper",
            "--run-id",
            run_id,
        ],
        cwd=Path.cwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    parent = child = -1
    try:
        _wait_for_path(adapter_pid)
        _wait_for_path(escaped_pid)
        parent = int(adapter_pid.read_text(encoding="utf-8"))
        child = int(escaped_pid.read_text(encoding="utf-8"))
        assert os.getpgid(parent) != os.getpgid(child)
        assert os.getsid(parent) == os.getsid(child)

        process.send_signal(engine_signal)
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode is not None and process.returncode != 0, (stdout, stderr)
        _wait_stopped(parent)
        _wait_stopped(child)

        reaped = [
            event
            for event in _journal_records(repo, run_id)
            if event.get("kind") == "worker_reaped"
        ]
        assert len(reaped) == 1
        assert reaped[0]["frame_id"] == "f0"
        assert reaped[0]["attempt"] == 0
        assert reaped[0]["session_id"] == parent
        assert reaped[0]["escalated"] is True
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        for pid in (parent, child):
            if pid > 0 and _process_is_running(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


@dataclass
class _FatalRig:
    pid_file: Path
    processes: list[subprocess.Popen[str]]
    timeout_s: float = 30.0

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, workdir
        assert runtime.worker is not None
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import signal,time; "
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                    "time.sleep(60)"
                ),
            ],
            start_new_session=True,
        )
        self.processes.append(process)
        runtime.worker.started(
            process.pid, os.getpgid(process.pid), os.getsid(process.pid)
        )
        self.pid_file.write_text(str(process.pid), encoding="utf-8")
        raise _FatalEngineError("fatal engine seam")


def test_fatal_engine_exception_reaps_and_journals_before_propagating(
    repo: Path, tmp_path: Path
) -> None:
    """BaseException propagation is outside, and strictly after, engine shutdown."""
    processes: list[subprocess.Popen[str]] = []
    rig = _FatalRig(tmp_path / "fatal.pid", processes)
    engine = Engine(
        tmp_path / "fatal-run",
        repo,
        RigRegistry({"fatal": rig}),
        run_id="fatal-worker-reap",
        root_rig="fatal",
        root_prompt="raise after worker launch",
        worktrees_root=tmp_path / "fatal-worktrees",
    )

    with pytest.raises(_FatalEngineError, match="fatal engine seam"):
        engine.run()

    assert len(processes) == 1
    process = processes[0]
    _wait_stopped(process.pid)
    process.wait(timeout=5)
    reaped = [
        event for event in engine.journal.events() if isinstance(event, WorkerReaped)
    ]
    assert len(reaped) == 1
    assert reaped[0].frame_id == "f0"
    assert reaped[0].attempt == 0
    assert reaped[0].session_id == process.pid
    assert reaped[0].escalated
