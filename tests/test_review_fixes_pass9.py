"""Pass-9 inplace sweep crash-window regression (hand-13)."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import NoReturn

from wildflows.engine import Engine, replay
from wildflows.expr import Edit, Inplace
from wildflows.rig import RigRegistry

from tests.test_review_fixes_pass5 import _base_repo
from tests.test_review_fixes_pass7 import _fork


def _exit_now(*_args: object, **_kwargs: object) -> NoReturn:
    os._exit(0)


def test_crash_after_each_inplace_leak_unlink_before_swept_marker_restarts_idempotently(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    targets = [workdir / "one.txt", workdir / "two.txt"]
    tree = Inplace(edits=[
        Edit(path="one.txt", content="ONE"),
        Edit(path="two.txt", content="TWO"),
    ])

    def die_after_writes() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _exit_now)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_writes) == 0

    for removed_count in (1, 2):
        def die_after_next_unlink() -> None:
            engine = Engine(run_dir, workdir, RigRegistry({}))
            real_fsync_dir = engine.ws._fsync_dir

            def fsync_then_die(path: Path) -> None:
                real_fsync_dir(path)
                if path == workdir and sum(not target.exists() for target in targets) == removed_count:
                    os._exit(0)

            setattr(engine.ws, "_fsync_dir", fsync_then_die)
            engine.run_epoch(tree, 0)

        assert _fork(die_after_next_unlink) == 0
        intent_path = next((run_dir / "intents").glob("*.json"))
        intent = json.loads(intent_path.read_text(encoding="utf-8"))
        assert sum(bool(write.get("swept")) for write in intent["writes"]) == removed_count
        assert sum(not target.exists() for target in targets) == removed_count

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert [target.read_text(encoding="utf-8") for target in targets] == ["ONE", "TWO"]
    assert replay(run_dir).epoch_closed(0)
