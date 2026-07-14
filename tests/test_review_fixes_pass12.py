"""Pass-12 regressions (hand-16)."""
from __future__ import annotations

import json
import os
import shlex
import signal
import stat
import time
from pathlib import Path

import pytest

import wildflows.journal as journal_module
from wildflows.engine import Engine, replay
from wildflows.events import Boundary
from wildflows.expr import Do, Inplace, Loop, RigRef, Until
from wildflows.journal import Journal
from wildflows.rig import RigRegistry, ShellRig
from wildflows.workspace import WorkspaceEffects

from tests.test_review_fixes_pass5 import _base_repo


def _fork_engine(run_dir: Path, workdir: Path, tree: Do | Loop, registry: RigRegistry) -> int:
    pid = os.fork()
    if pid == 0:
        try:
            Engine(run_dir, workdir, registry).run_epoch(tree, 0)
        finally:
            os._exit(90)
    return pid


def _wait_for(path: Path, message: str) -> None:
    deadline = time.monotonic() + 10
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not path.exists():
        pytest.fail(message)


def _kill_and_wait(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:
        pass


def _wait_process_absent(pid: int) -> None:
    deadline = time.monotonic() + 5
    stat_path = Path(f"/proc/{pid}/stat")
    while stat_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if stat_path.exists():
        pytest.fail(f"process {pid} did not disappear")


def _process_identity(pid: int) -> tuple[str, str] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except FileNotFoundError:
        return None
    fields = raw.rpartition(")")[2].split()
    return fields[19], fields[0]


def _same_process_is_live(pid: int, start_time: str) -> bool:
    current = _process_identity(pid)
    return current is not None and current[0] == start_time and current[1] != "Z"


def test_killed_engine_reaps_predicate_group_after_foreground_exits_and_result_pipe_breaks(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    marker = tmp_path / "first-attempt"
    delayed_started = tmp_path / "delayed-writer-started"
    foreground_exit = tmp_path / "allow-foreground-exit"
    cmd = (
        f"if test ! -e {shlex.quote(str(marker))}; then "
        f": > {shlex.quote(str(marker))}; "
        f"(: > {shlex.quote(str(delayed_started))}; sleep 1.5; "
        f"printf ORPHAN > {shlex.quote(str(workdir / 'base.txt'))}) & "
        f"while test ! -e {shlex.quote(str(foreground_exit))}; do sleep 0.01; done; "
        "else test \"$(cat base.txt)\" = base; fi"
    )
    tree = Loop(
        body=Inplace(edits=[]), until=Until(kind="cmd", cmd=cmd, timeout_s=30), cap=1
    )

    engine_pid = _fork_engine(run_dir, workdir, tree, RigRegistry({}))
    try:
        _wait_for(delayed_started, "predicate did not start")
        records = [
            *list((run_dir / "processes").glob("*.json")),
            *list((run_dir / "predicate-processes").glob("*.json")),
        ]
        assert len(records) == 1
        supervisor_pid = int(json.loads(records[0].read_text())["pid"])
    finally:
        _kill_and_wait(engine_pid)
        foreground_exit.touch()  # release even when setup assertions fail
    _wait_process_absent(supervisor_pid)

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert replay(run_dir).epoch_closed(0)
    assert (workdir / "base.txt").read_bytes() == b"base"
    time.sleep(1.7)
    assert (workdir / "base.txt").read_bytes() == b"base"
    assert not list((run_dir / "processes").glob("*.json"))


def test_killed_engine_reaps_inflight_shell_rig_before_recovery_and_after_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    marker = tmp_path / "first-rig-attempt"
    delayed_started = tmp_path / "rig-delayed-writer-started"
    delayed_pid = tmp_path / "rig-delayed-writer-pid"
    command = (
        f"if test ! -e {shlex.quote(str(marker))}; then "
        f": > {shlex.quote(str(marker))}; "
        f"(: > {shlex.quote(str(delayed_started))}; sleep 1.2; "
        f"printf ORPHAN > {shlex.quote(str(workdir / 'base.txt'))}) & "
        f"echo $! > {shlex.quote(str(delayed_pid))}; sleep 20; "
        "else test \"$(cat base.txt)\" = base; fi"
    )
    tree = Do(task="mutate once", rig=RigRef(name="shell"))
    registry = RigRegistry({"shell": ShellRig(command, timeout_s=30)})

    engine_pid = _fork_engine(run_dir, workdir, tree, registry)
    try:
        _wait_for(delayed_started, "shell rig did not start")
        _wait_for(delayed_pid, "shell rig did not publish its background pid")
    finally:
        _kill_and_wait(engine_pid)
    assert (workdir / "base.txt").read_bytes() == b"base"
    old_child = int(delayed_pid.read_text().strip())
    child_identity = _process_identity(old_child)
    assert child_identity is not None
    old_child_start = child_identity[0]
    original_reap = WorkspaceEffects.reap_process
    reaped_before_recovery = False

    def assert_reaped(
        ws: WorkspaceEffects, epoch: int, node_id: str, attempt: int,
    ) -> None:
        nonlocal reaped_before_recovery
        original_reap(ws, epoch, node_id, attempt)
        if node_id == "n0" and attempt == 0:
            assert not _same_process_is_live(old_child, old_child_start)
            reaped_before_recovery = True

    monkeypatch.setattr(WorkspaceEffects, "reap_process", assert_reaped)
    Engine(run_dir, workdir, registry).run_epoch(tree, 0)
    assert reaped_before_recovery
    assert replay(run_dir).epoch_closed(0)
    assert (workdir / "base.txt").read_bytes() == b"base"
    time.sleep(1.4)
    assert (workdir / "base.txt").read_bytes() == b"base"
    assert not _same_process_is_live(old_child, old_child_start)
    assert not list((run_dir / "processes").glob("*.json"))


@pytest.mark.parametrize("failed_sync", ["file", "first-file-directory"])
def test_fresh_load_fsyncs_complete_tail_after_failed_append_fsync_before_accepting_closed_epoch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_sync: str,
) -> None:
    run_dir = tmp_path / "run"
    journal = Journal(run_dir)
    event = Boundary(run_id="run", epoch=0, node_id="n0", phase="closed")
    with monkeypatch.context() as injected:
        if failed_sync == "file":
            def fail_fsync(_fd: int) -> None:
                raise OSError("injected file fsync failure")

            injected.setattr(os, "fsync", fail_fsync)
        else:
            def fail_directory(_path: Path) -> None:
                raise OSError("injected first-file directory fsync failure")

            injected.setattr(journal_module, "_fsync_directory", fail_directory)
        with pytest.raises(OSError, match="injected"):
            journal.append(event)

    synced_modes: list[int] = []
    real_fsync = os.fsync

    def record_fsync(fd: int) -> None:
        synced_modes.append(os.fstat(fd).st_mode)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", record_fsync)
    fresh = Journal.load(run_dir)
    assert fresh.projection.epoch_closed(0)
    assert any(stat.S_ISREG(mode) for mode in synced_modes)
    assert any(stat.S_ISDIR(mode) for mode in synced_modes)


def test_reaper_treats_esrch_between_proc_identity_and_getpgid_as_an_absent_or_rechecked_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from wildflows.workspace import ProcessRecord

    ws = WorkspaceEffects(tmp_path / "work", tmp_path / "run")
    record = ProcessRecord(
        epoch=0, node_id="n0", attempt=0, pid=4242, pgid=4242, start_time=100
    )
    record.integrity = ws._record_integrity(record)
    path = ws._process_path(0, "n0", 0)
    ws._fsync_json(path, record)
    monkeypatch.setattr(
        ws, "_proc_identity", lambda pid: (100, "R", 4242) if pid == 4242 else None
    )

    def gone(_pid: int) -> int:
        raise ProcessLookupError

    monkeypatch.setattr(os, "getpgid", gone)
    monkeypatch.setattr(ws, "_group_processes", lambda _record: [])
    ws.reap_process(0, "n0", 0)
    assert not path.exists()


def test_reaper_never_signals_a_recycled_recorded_leader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from wildflows.workspace import ProcessRecord

    ws = WorkspaceEffects(tmp_path / "work", tmp_path / "run")
    record = ProcessRecord(
        epoch=0, node_id="n0", attempt=0, pid=4343, pgid=4343, start_time=100
    )
    record.integrity = ws._record_integrity(record)
    path = ws._process_path(0, "n0", 0)
    ws._fsync_json(path, record)
    monkeypatch.setattr(ws, "_proc_identity", lambda _pid: (101, "R", 4343))

    def must_not_open(_pid: int) -> int:
        pytest.fail("reaper opened a recycled process generation")

    monkeypatch.setattr(os, "pidfd_open", must_not_open)
    monkeypatch.setattr(
        ws, "_group_processes",
        lambda _record: pytest.fail("recycled group must not be enumerated"),
    )
    ws.reap_process(0, "n0", 0)
    assert not path.exists()


@pytest.mark.parametrize("race", ["pid-reused-after-scan", "exit-before-pidfd-signal"])
def test_reaper_rechecks_enumerated_identity_and_retries_pidfd_esrch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, race: str,
) -> None:
    from wildflows.workspace import ProcessRecord

    ws = WorkspaceEffects(tmp_path / "work", tmp_path / "run")
    record = ProcessRecord(
        epoch=0, node_id="n0", attempt=0, pid=4444, pgid=4444, start_time=100
    )
    record.integrity = ws._record_integrity(record)
    path = ws._process_path(0, "n0", 0)
    ws._fsync_json(path, record)
    monkeypatch.setattr(
        ws, "_proc_identity",
        lambda pid: None if pid == 4444 else (
            101 if race == "pid-reused-after-scan" else 100, "R", 4444
        ),
    )
    groups = [[(5555, 100)], []]
    monkeypatch.setattr(ws, "_group_processes", lambda _record: groups.pop(0))
    monkeypatch.setattr(os, "pidfd_open", lambda _pid: os.open("/dev/null", os.O_RDONLY))

    def signal_race(_pidfd: int, _sig: int) -> None:
        if race == "pid-reused-after-scan":
            pytest.fail("reaper signalled a PID generation that changed after its scan")
        raise ProcessLookupError

    monkeypatch.setattr(signal, "pidfd_send_signal", signal_race)
    ws.reap_process(0, "n0", 0)
    assert not path.exists()
