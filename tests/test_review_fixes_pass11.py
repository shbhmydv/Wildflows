"""Pass-11 regressions (hand-15)."""
from __future__ import annotations

import builtins
import json
import os
import shlex
import signal
import subprocess
import time
from pathlib import Path
from typing import IO, cast

import pytest

import wildflows.journal as journal_module
from wildflows.engine import Engine, PredicateEvaluationError, replay
from wildflows.events import Boundary
from wildflows.expr import Inplace, Loop, Until
from wildflows.journal import Journal, JournalExistsError
from wildflows.rig import RigRegistry

from tests.test_review_fixes_pass5 import _base_repo
from tests.test_review_fixes_pass7 import _capture_bytes


def _index_path(workdir: Path) -> Path:
    raw = subprocess.run(
        ["git", "rev-parse", "--git-path", "index"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    path = Path(raw)
    return path if path.is_absolute() else workdir / path


def test_predicate_cannot_hide_tracked_mutation_with_index_flags(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    original_index = _index_path(workdir).read_bytes()
    tree = Loop(
        body=Inplace(edits=[]),
        until=Until(
            kind="cmd",
            cmd="git update-index --assume-unchanged base.txt; printf HIDDEN > base.txt",
        ),
        cap=1,
    )

    with pytest.raises(PredicateEvaluationError, match="read-only verification"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)

    assert (workdir / "base.txt").read_bytes() == b"base"
    assert _index_path(workdir).read_bytes() == original_index
    assert _capture_bytes(run_dir, "base.txt") == [b"HIDDEN"]
    failed = replay(run_dir).node((0, "n0.until")).result
    assert failed is not None and not failed.ok
    assert not replay(run_dir).epoch_closed(0)


def test_clean_predicate_restores_index_after_postcondition_git_reads(tmp_path: Path) -> None:
    workdir = tmp_path / "work-touch"
    _base_repo(workdir)
    run_dir = tmp_path / "run-touch"
    original_index = _index_path(workdir).read_bytes()
    tree = Loop(
        body=Inplace(edits=[]),
        until=Until(kind="cmd", cmd="touch base.txt; true"),
        cap=1,
    )

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert replay(run_dir).epoch_closed(0)
    assert _index_path(workdir).read_bytes() == original_index


def test_predicate_verification_compares_unfiltered_tracked_bytes(tmp_path: Path) -> None:
    workdir = tmp_path / "work-filter"
    _base_repo(workdir)
    (workdir / ".gitattributes").write_text("base.txt text eol=lf\n", encoding="utf-8")
    (workdir / "base.txt").write_bytes(b"base\n")
    subprocess.run(["git", "add", "-A"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", "filtered baseline"], cwd=workdir, check=True)
    run_dir = tmp_path / "run-filter"
    tree = Loop(
        body=Inplace(edits=[]),
        until=Until(kind="cmd", cmd="printf 'base\\r\\n' > base.txt"),
        cap=1,
    )

    with pytest.raises(PredicateEvaluationError):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert (workdir / "base.txt").read_bytes() == b"base\n"
    assert _capture_bytes(run_dir, "base.txt") == [b"base\r\n"]
    assert not replay(run_dir).epoch_closed(0)


def test_killed_engine_reaps_inflight_predicate_before_recovery(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    marker = tmp_path / "predicate-started"
    delayed_started = tmp_path / "delayed-writer-started"
    delayed_write = (
        f"(: > {shlex.quote(str(delayed_started))}; sleep 1; "
        f"printf ORPHAN > {shlex.quote(str(workdir / 'base.txt'))}) & sleep 20"
    )
    cmd = (
        f"if test ! -e {shlex.quote(str(marker))}; then : > {shlex.quote(str(marker))}; "
        f"{delayed_write}; else test \"$(cat base.txt)\" = base; fi"
    )
    tree = Loop(
        body=Inplace(edits=[]), until=Until(kind="cmd", cmd=cmd, timeout_s=30), cap=1
    )

    pid = os.fork()
    if pid == 0:
        try:
            Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
        finally:
            os._exit(90)
    deadline = time.monotonic() + 10
    while not delayed_started.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not delayed_started.exists():
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        pytest.fail("predicate did not start")
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    assert (workdir / "base.txt").read_bytes() == b"base"

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert replay(run_dir).epoch_closed(0)
    assert (workdir / "base.txt").read_bytes() == b"base"
    time.sleep(1.2)
    assert (workdir / "base.txt").read_bytes() == b"base"
    assert not list((run_dir / "predicate-processes").glob("*.json"))


class _PartialWriter:
    def __init__(self, inner: IO[str], valid: bool) -> None:
        self.inner = inner
        self.valid = valid

    def __enter__(self) -> "_PartialWriter":
        return self

    def __exit__(self, *_args: object) -> None:
        self.inner.close()

    def write(self, text: str) -> int:
        cut = len(text) - 1 if self.valid else 23
        self.inner.write(text[:cut])
        self.inner.flush()
        raise OSError("injected partial append")

    def flush(self) -> None:
        self.inner.flush()

    def fileno(self) -> int:
        return self.inner.fileno()


@pytest.mark.parametrize("valid_tail", [False, True])
def test_partial_append_failure_fresh_load_repairs_tail_before_continuing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, valid_tail: bool,
) -> None:
    run_dir = tmp_path / "run"
    journal = Journal(run_dir)
    journal.append(Boundary(run_id="run", epoch=0, node_id="n0", phase="opened"))
    real_open = builtins.open

    def partial_open(
        path: int | str | bytes | os.PathLike[str] | os.PathLike[bytes],
        mode: str = "r", encoding: str | None = None,
    ) -> _PartialWriter:
        inner = cast(IO[str], real_open(path, mode, encoding=encoding))
        return _PartialWriter(inner, valid_tail)

    monkeypatch.setattr(journal_module, "open", partial_open, raising=False)
    with pytest.raises(OSError, match="partial append"):
        journal.append(Boundary(run_id="run", epoch=0, node_id="n0", phase="closed"))
    monkeypatch.delattr(journal_module, "open")
    directory_fsyncs: list[Path] = []
    real_fsync_directory = journal_module._fsync_directory

    def record_directory_fsync(path: Path) -> None:
        directory_fsyncs.append(path)
        real_fsync_directory(path)

    monkeypatch.setattr(journal_module, "_fsync_directory", record_directory_fsync)
    fresh = Journal.load(run_dir)
    assert [event.seq for event in fresh.events()] == [0]
    assert directory_fsyncs == [run_dir]
    assert (run_dir / "events.ndjson").read_bytes().endswith(b"\n")
    assert fresh.append(
        Boundary(run_id="run", epoch=0, node_id="n0", phase="closed")
    ) == 1
    assert [event.seq for event in Journal.load(run_dir).events()] == [0, 1]


def test_load_rejects_complete_blank_middle_record(tmp_path: Path) -> None:
    run_dir = tmp_path / "blank-run"
    journal = Journal(run_dir)
    journal.append(Boundary(run_id="run", epoch=0, node_id="n0", phase="opened"))
    with open(run_dir / "events.ndjson", "ab") as fh:
        fh.write(b"\n")
    journal.append(Boundary(run_id="run", epoch=0, node_id="n0", phase="closed"))
    with pytest.raises(json.JSONDecodeError):
        Journal.load(run_dir)


def test_constructor_refuses_existing_nonempty_journal(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    Journal(run_dir).append(
        Boundary(run_id="run", epoch=0, node_id="n0", phase="opened")
    )
    with pytest.raises(JournalExistsError, match="Journal.load"):
        Journal(run_dir)
