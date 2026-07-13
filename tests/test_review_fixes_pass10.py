"""Pass-10 predicate and journal regressions (hand-14)."""
from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

import pytest

from wildflows.engine import Engine, PredicateEvaluationError, replay
from wildflows.events import Event, LoopIter
from wildflows.expr import Edit, Inplace, Loop, Until
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
