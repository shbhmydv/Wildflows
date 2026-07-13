"""End-to-end: the mind-steers loop over a do/inplace tree, journalled + replayable."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from wildflows.admission import AdmissionError
from wildflows.engine import Engine, replay
from wildflows.expr import Ask, Do, Edit, Inplace, Loop, RigRef, Seq, Until
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
    tree = Seq(
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
    tree = Seq(
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
    assert (0, inplace_id) in state.integrated
    assert (0, do_id) in state.results
    assert state.results[(0, do_id)].ok is True
    assert "t" in state.results[(0, do_id)].text


def test_unexecutable_primitive_rejected_at_admission(tmp_path: Path) -> None:
    # A representable-but-not-executable kind is rejected BEFORE any epoch opens (item 5),
    # not with a NotImplementedError after a durable boundary.
    eng = _engine(tmp_path)
    with pytest.raises(AdmissionError):
        eng.run_epoch(Ask(question="which?"), epoch=0)
    assert not (eng.run_dir / "events.ndjson").exists()  # no incomplete epoch was opened


def test_inplace_rejects_sibling_prefix_escape(tmp_path: Path) -> None:
    # A pure string-prefix check would ACCEPT '../work-evil/f' because its resolved
    # path shares the '/.../work' textual prefix. is_relative_to must reject it.
    eng = _engine(tmp_path)
    sibling = eng.workdir.parent / "work-evil"
    sibling.mkdir()
    with pytest.raises(ValueError, match="escapes workdir"):
        eng.run_epoch(Inplace(edits=[Edit(path="../work-evil/f", content="x")]), epoch=0)
    assert not (sibling / "f").exists()  # nothing was written outside the workdir


def _counter_loop_body() -> Seq:
    return Seq(
        children=[
            Inplace(edits=[Edit(path="marker.txt", content="body ran")]),
            Do(task="tick", rig=RigRef(name="echo")),
        ]
    )


# Predicate maintains a counter file and converges once it reaches N.
_CONVERGE_AT_3 = "c=$(cat counter 2>/dev/null || echo 0); c=$((c+1)); echo $c > counter; test $c -ge 3"


def test_loop_converges_and_journals_iterations(tmp_path: Path) -> None:
    eng = _engine(tmp_path)
    loop = Loop(body=_counter_loop_body(), until=Until(kind="cmd", cmd=_CONVERGE_AT_3), cap=10)
    eng.run_epoch(loop, epoch=0)

    iters = [e for e in eng.journal.events() if e.kind == "loop_iter"]
    assert len(iters) == 3  # converged on the 3rd
    assert iters[-1].converged is True
    result = [e for e in eng.journal.events() if e.kind == "result" and e.node_id == "n0"]
    assert result[-1].ok is True
    # SF6: convergence/cap status is in loop_status; text/files carry the body artifact.
    assert "converged after 3" in (result[-1].loop_status or "")
    assert "converged" not in result[-1].text  # the artifact, not the prose
    # body effect landed + was committed each iteration
    assert (eng.workdir / "marker.txt").read_text() == "body ran"
    state = replay(eng.run_dir)
    assert state.loop_iterations[(0, "n0")] == 3
    assert state.loop_last_commit[(0, "n0")] is not None


def test_loop_cap_exhaustion_is_result_not_crash(tmp_path: Path) -> None:
    eng = _engine(tmp_path)
    # `false` never converges -> loop runs exactly cap iterations, ok=False, no raise.
    loop = Loop(body=_counter_loop_body(), until=Until(kind="cmd", cmd="false"), cap=3)
    eng.run_epoch(loop, epoch=0)

    iters = [e for e in eng.journal.events() if e.kind == "loop_iter"]
    assert len(iters) == 3
    assert all(e.converged is False for e in iters)
    result = [e for e in eng.journal.events() if e.kind == "result" and e.node_id == "n0"]
    assert result[-1].ok is False
    assert "hit cap 3" in (result[-1].loop_status or "")
    kinds = [e.kind for e in eng.journal.events()]
    assert kinds[-1] == "boundary"  # epoch still closes cleanly


def test_loop_flag_predicate_rejected_at_admission(tmp_path: Path) -> None:
    eng = _engine(tmp_path)
    loop = Loop(body=_counter_loop_body(), until=Until(kind="flag"), cap=3)
    with pytest.raises(AdmissionError):  # flag predicate not executable yet (item 5)
        eng.run_epoch(loop, epoch=0)


def test_replay_folds_mid_loop_journal(tmp_path: Path) -> None:
    eng = _engine(tmp_path)
    loop = Loop(body=_counter_loop_body(), until=Until(kind="cmd", cmd=_CONVERGE_AT_3), cap=10)
    eng.run_epoch(loop, epoch=0)

    # Truncate the ndjson after the 2nd loop_iter (simulate a kill mid-loop).
    path = eng.run_dir / "events.ndjson"
    lines = path.read_text().splitlines()
    seen = 0
    cut = len(lines)
    for i, ln in enumerate(lines):
        if '"kind":"loop_iter"' in ln.replace(" ", ""):
            seen += 1
            if seen == 2:
                cut = i + 1
                break
    path.write_text("\n".join(lines[:cut]) + "\n")

    state = replay(eng.run_dir)
    assert state.loop_iterations[(0, "n0")] == 2  # iterations-completed == k
    assert state.loop_last_commit[(0, "n0")] is not None
    assert not state.epoch_closed(0)  # kill happened before the boundary closed
