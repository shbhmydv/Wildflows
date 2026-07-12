"""End-to-end: the mind-steers loop over a do/inplace tree, journalled + replayable."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from wildflows.engine import Engine, replay
from wildflows.expr import Ask, Dispatch, Do, Edit, Inplace, RigRef
from wildflows.rig import EchoRig, RigRegistry


def _git_init(workdir: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=workdir, check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=workdir, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=workdir, check=True)


def _engine(tmp_path: Path) -> Engine:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    reg = RigRegistry({"echo": EchoRig()})
    return Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg)


def test_inplace_then_do_end_to_end(tmp_path: Path) -> None:
    eng = _engine(tmp_path)
    tree = Dispatch(
        children=[
            Inplace(edits=[Edit(path="hello.txt", content="hi from planner")]),
            Do(task="summarize hello.txt", rig=RigRef(name="echo")),
        ]
    )
    eng.run_epoch(tree, epoch=0)

    # inplace effect landed and was committed by the core
    assert (eng.workdir / "hello.txt").read_text() == "hi from planner"
    log = subprocess.run(
        ["git", "log", "--oneline"], cwd=eng.workdir, capture_output=True, text=True
    ).stdout
    assert log.strip()  # at least one commit exists

    # every primitive execution is journalled: boundary(open/close), dispatched, result, integrated
    kinds = [e.kind for e in eng.journal.events()]
    assert kinds[0] == "boundary"
    assert kinds[-1] == "boundary"
    assert "dispatched" in kinds
    assert "result" in kinds
    assert "integrated" in kinds  # inplace commit


def test_replay_reconstructs_state_from_ndjson_alone(tmp_path: Path) -> None:
    eng = _engine(tmp_path)
    tree = Dispatch(
        children=[
            Inplace(edits=[Edit(path="a.txt", content="x")]),
            Do(task="t", rig=RigRef(name="echo")),
        ]
    )
    eng.run_epoch(tree, epoch=0)
    run_dir = eng.run_dir

    # reconstruct purely from disk
    state = replay(run_dir)
    assert state.epoch_closed(0)
    # inplace node integrated; do node has a result
    inplace_id = "n0.0"
    do_id = "n0.1"
    assert inplace_id in state.integrated
    assert do_id in state.results
    assert state.results[do_id].ok is True
    assert "t" in state.results[do_id].text


def test_unexecutable_primitive_raises_in_poc(tmp_path: Path) -> None:
    eng = _engine(tmp_path)
    with pytest.raises(NotImplementedError):
        eng.run_epoch(Ask(question="which?"), epoch=0)
