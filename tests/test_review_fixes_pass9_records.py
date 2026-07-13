"""Pass-9 required-record result-tear regressions (hand-13).

Each torn completion uses a real ``fork``/``os._exit`` child; restart assertions consume
only the journal and durable records left by the dead process.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import NoReturn

import pytest

from wildflows.engine import Engine, replay
from wildflows.expr import Do, Edit, Inplace, RigRef
from wildflows.result import IntegrationReceipt
from wildflows.rig import RigRegistry, Result
from wildflows.workspace import WorkspaceFault

from tests.test_review_fixes import _CountingRig
from tests.test_review_fixes_pass5 import _base_repo
from tests.test_review_fixes_pass7 import _events, _fork


def _exit_now(*_args: object, **_kwargs: object) -> NoReturn:
    os._exit(0)


class _CommitEffect:
    def run(self, prompt: str, workdir: Path) -> Result:
        (workdir / "effect.txt").write_text("effect", encoding="utf-8")
        subprocess.run(["git", "add", "effect.txt"], cwd=workdir, check=True)
        subprocess.run(["git", "commit", "-qm", "effect"], cwd=workdir, check=True)
        return Result(text="committed")


def _leave_do_result_tear(run_dir: Path, workdir: Path, tree: Do) -> None:
    def die_after_result() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"commit": _CommitEffect()}))

        def record_result_then_die(
            key: tuple[int, str], result: Result, receipt: IntegrationReceipt,
            post_head: str | None = None,
        ) -> NoReturn:
            engine.rec.record_result(key, result, post_head=post_head, receipt_required=True)
            os._exit(0)

        setattr(engine.rec, "record_success", record_result_then_die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_result) == 0


def test_missing_required_lease_on_result_integrated_tear_fails_closed(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="commit", rig=RigRef(name="commit"))

    _leave_do_result_tear(run_dir, workdir, tree)
    next((run_dir / "leases").glob("*.json")).unlink()
    rerun = _CountingRig("must not run")

    with pytest.raises(WorkspaceFault, match="required.*lease|lease.*required"):
        Engine(run_dir, workdir, RigRegistry({"commit": rerun})).run_epoch(tree, 0)

    assert rerun.calls == 0
    assert not any(event["kind"] == "integrated" for event in _events(run_dir))
    state = replay(run_dir)
    assert state.node((0, "n0")).workspace_unclean is True
    assert not state.epoch_closed(0)


def test_crash_after_result_tear_settlement_before_integrated_resumes_from_certificate(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="commit", rig=RigRef(name="commit"))
    _leave_do_result_tear(run_dir, workdir, tree)

    def die_before_integrated() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"commit": _CountingRig("not-run")}))
        setattr(engine.rec, "record_integrated", _exit_now)
        engine.run_epoch(tree, 0)

    assert _fork(die_before_integrated) == 0
    assert not list((run_dir / "leases").glob("*.json"))
    assert list((run_dir / "settlements").glob("*.json"))
    assert not any(event["kind"] == "integrated" for event in _events(run_dir))

    rerun = _CountingRig("must-not-run")
    Engine(run_dir, workdir, RigRegistry({"commit": rerun})).run_epoch(tree, 0)
    assert rerun.calls == 0
    assert replay(run_dir).epoch_closed(0)
    assert any(event["kind"] == "integrated" for event in _events(run_dir))


def test_missing_required_intent_on_inplace_result_integrated_tear_fails_closed(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="new.txt", content="NEW")])

    def die_after_result() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))

        def record_result_then_die(
            key: tuple[int, str], result: Result, receipt: IntegrationReceipt,
            post_head: str | None = None,
        ) -> NoReturn:
            engine.rec.record_result(key, result, post_head=post_head, receipt_required=True)
            os._exit(0)

        setattr(engine.rec, "record_success", record_result_then_die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_result) == 0
    next((run_dir / "intents").glob("*.json")).unlink()

    with pytest.raises(WorkspaceFault, match="required.*intent|intent.*required"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)

    assert not any(event["kind"] == "integrated" for event in _events(run_dir))
    state = replay(run_dir)
    assert state.node((0, "n0")).workspace_unclean is True
    assert not state.epoch_closed(0)
