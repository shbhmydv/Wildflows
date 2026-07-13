"""Pass-10 predicate and journal regressions (hand-14)."""
from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

import pytest

from wildflows.engine import Engine, PredicateEvaluationError, replay
from wildflows.events import Boundary, Event, LoopIter
from wildflows.expr import Edit, Inplace, Loop, Until
from wildflows.journal import Journal, JournalPoisonedError
from wildflows.rig import RigRegistry

from tests.test_review_fixes_pass5 import _base_repo
from tests.test_review_fixes_pass7 import _fork


def test_loop_predicate_effect_is_mediated_and_predicate_to_loop_iter_crash_is_redo_safe(
    tmp_path: Path,
) -> None:
    """A predicate mutation is reverted+failed; a clean predicate may redo before LoopIter."""
    workdir = tmp_path / "mutating-work"
    _base_repo(workdir)
    run_dir = tmp_path / "mutating-run"
    mutating = Loop(
        body=Inplace(edits=[Edit(path="body.txt", content="BODY")]),
        until=Until(kind="cmd", cmd="printf MUTATED > base.txt; true"),
        cap=1,
    )

    with pytest.raises(PredicateEvaluationError, match="predicate"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(mutating, 0)

    assert (workdir / "base.txt").read_text(encoding="utf-8") == "base"
    assert subprocess.run(
        ["git", "status", "--porcelain"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout == ""
    failed = replay(run_dir).node((0, "n0.until")).result
    assert failed is not None and not failed.ok
    assert not replay(run_dir).epoch_closed(0)
    assert list((run_dir / "failed-diffs").glob("*.capture/manifest.json"))

    redo_workdir = tmp_path / "redo-work"
    _base_repo(redo_workdir)
    redo_run_dir = tmp_path / "redo-run"
    outside_counter = tmp_path / "outside-counter"
    check = (
        f"printf X >> {shlex.quote(str(outside_counter))}; "
        'test "$(cat base.txt)" = base'
    )
    redo = Loop(
        body=Inplace(edits=[Edit(path="body.txt", content="BODY")]),
        until=Until(kind="cmd", cmd=check),
        cap=1,
    )

    def die_before_loop_iter() -> None:
        engine = Engine(redo_run_dir, redo_workdir, RigRegistry({}))
        real_append = engine.journal.append

        def append_or_die(event: Event) -> int:
            if isinstance(event, LoopIter):
                os._exit(0)
            return real_append(event)

        setattr(engine.journal, "append", append_or_die)
        engine.run_epoch(redo, 0)

    assert _fork(die_before_loop_iter) == 0
    assert outside_counter.read_text(encoding="utf-8") == "X"
    assert (redo_workdir / "base.txt").read_text(encoding="utf-8") == "base"

    Engine(redo_run_dir, redo_workdir, RigRegistry({})).run_epoch(redo, 0)
    state = replay(redo_run_dir)
    assert outside_counter.read_text(encoding="utf-8") == "XX"
    assert state.node((0, "n0.0")).dispatch_count == 1  # body did not rerun
    assert state.node((0, "n0.until")).dispatch_count == 2  # predicate did redo
    assert state.epoch_closed(0)
    assert subprocess.run(
        ["git", "status", "--porcelain"], cwd=redo_workdir, check=True,
        capture_output=True, text=True,
    ).stdout == ""


def test_append_io_failure_poison_or_retry_cannot_create_sequence_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late append failure leaves no live-memory event and poisons that owner."""
    journal = Journal(tmp_path / "run")
    event = Boundary(run_id="r", epoch=0, node_id="n0", phase="opened")
    real_fsync = os.fsync

    def fail_fsync(_fd: int) -> None:
        raise OSError("injected journal fsync failure")

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(OSError, match="injected"):
        journal.append(event)
    assert journal.events() == []
    assert event.seq == -1

    monkeypatch.setattr(os, "fsync", real_fsync)
    with pytest.raises(JournalPoisonedError, match="fresh Journal"):
        journal.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="closed"))

    # The flushed line may in fact be durable after a failed fsync.  Only a fresh load
    # decides that physical tail, then continues contiguously; the poisoned owner cannot.
    reloaded = Journal.load(tmp_path / "run")
    assert [item.seq for item in reloaded.events()] == [0]
    assert reloaded.append(
        Boundary(run_id="r", epoch=0, node_id="n0", phase="closed")
    ) == 1
    assert [item.seq for item in Journal.load(tmp_path / "run").events()] == [0, 1]
