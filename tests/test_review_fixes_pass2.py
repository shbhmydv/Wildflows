"""Regression tests from the external adversarial review, PASS 2 (hand-5).

Each test carries the reviewer's exact name. They exercise the pass-2 findings: resume
tree identity, node-instance dealiasing, the loop_iter crash window, the do
commit-before-journal window, failed-do / rig-commit effect handling, ctx file
containment, and the seven SHOULD-FIXes.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from wildflows.engine import Engine, replay
from wildflows.events import Boundary
from wildflows.expr import (
    CtxRef,
    Do,
    Edit,
    Inplace,
    Loop,
    RigRef,
    Seq,
    Until,
    assign_node_ids,
    parse_expr,
)
from wildflows.journal import Journal
from wildflows.rig import EchoRig, RigRegistry, ShellRig

from tests.test_review_fixes import _CountingRig, _git_init, _shell_reg


def _events(run_dir: Path) -> list[dict[str, object]]:
    return [json.loads(ln) for ln in (run_dir / "events.ndjson").read_text().splitlines()]


def _truncate_before_kinds(run_dir: Path, *kinds: str) -> None:
    """Drop the trailing tail of the journal starting at the FIRST event whose kind is
    in `kinds` after the earliest match — used to simulate a crash mid-write."""
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    cut = len(lines)
    for i, ln in enumerate(lines):
        if json.loads(ln)["kind"] in kinds:
            cut = i
            break
    (run_dir / "events.ndjson").write_text("\n".join(lines[:cut]) + ("\n" if cut else ""))


# --------------------------------------------------------------------------- B1

def test_same_engine_second_run_epoch_is_a_noop(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    rig = _CountingRig("do")
    eng = Engine(run_dir=tmp_path / "run", workdir=workdir,
                 registry=RigRegistry({"c": rig}))
    tree = Do(task="work", rig=RigRef(name="c"))
    eng.run_epoch(tree, epoch=0)
    eng.run_epoch(tree, epoch=0)  # same live Engine, closed epoch -> must be a no-op
    assert rig.calls == 1
    kinds = [e["kind"] for e in _events(tmp_path / "run")]
    assert kinds.count("boundary") == 2  # exactly one opened + one closed, not two pairs


# --------------------------------------------------------------------------- B2 / SF1

def test_load_handles_mid_utf8_unterminated_tail_and_rejects_newline_terminated_invalid_tail(
    tmp_path: Path,
) -> None:
    good = json.dumps({"seq": 0, "kind": "boundary", "run_id": "r", "epoch": 0,
                       "node_id": "n0", "phase": "opened"})
    # (a) A kill mid-multibyte-UTF-8 on the UNTERMINATED final record -> recovered.
    path = tmp_path / "a" / "events.ndjson"
    path.parent.mkdir(parents=True)
    with open(path, "wb") as fh:
        fh.write(good.encode() + b"\n")
        fh.write(b'{"kind":"result","text":"\xe2\x9c')  # truncated 3-byte char, no newline
    reloaded = Journal.load(tmp_path / "a")
    assert [e.kind for e in reloaded.events()] == ["boundary"]

    # (b) A COMPLETE (newline-terminated) but invalid final record is corruption -> raise.
    path2 = tmp_path / "b" / "events.ndjson"
    path2.parent.mkdir(parents=True)
    path2.write_text(good + "\n" + '{"kind":"result"}\n', encoding="utf-8")  # missing `ok`
    with pytest.raises((json.JSONDecodeError, ValidationError)):
        Journal.load(tmp_path / "b")


# --------------------------------------------------------------------------- NB1

def test_resume_rejects_a_tree_different_from_the_open_boundary_expr(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    admitted = parse_expr(Seq(children=[
        Do(task="A", rig=RigRef(name="c")),
        Do(task="B", rig=RigRef(name="c")),
    ]).model_dump())
    assign_node_ids(admitted)
    # An epoch was OPENED (its admitted tree journalled) but never closed.
    Journal(run_dir).append(
        Boundary(run_id="run", epoch=0, node_id="n0", phase="opened", expr=admitted.model_dump())
    )
    divergent = Seq(children=[
        Do(task="A", rig=RigRef(name="c")),
        Do(task="C", rig=RigRef(name="c")),  # planner re-shaped mid-epoch: illegal
    ])
    eng = Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": _CountingRig("c")}))
    with pytest.raises(ValueError, match="differs from the admitted boundary"):
        eng.run_epoch(divergent, epoch=0)


# --------------------------------------------------------------------------- NB2

def test_assign_node_ids_rejects_or_dealiases_reused_expression_instances_and_resume_runs_both_positions(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    rig = _CountingRig("do")
    reg = RigRegistry({"c": rig})
    d = Do(task="same", rig=RigRef(name="c"))
    tree = Seq(children=[d, d])  # the SAME python instance in both positions

    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(tree, epoch=0)
    assert rig.calls == 2  # both positions ran, not collapsed onto one node_id
    node_ids = {e["node_id"] for e in _events(run_dir) if e["kind"] == "result"}
    assert {"n0.0", "n0.1"} <= node_ids

    # Resume proof: drop the second position + close, keeping only the first, and reset
    # git past the first commit; the second position must still run on resume.
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    keep = []
    seen_results = 0
    for ln in lines:
        keep.append(ln)
        if json.loads(ln)["kind"] == "integrated":
            seen_results += 1
            if seen_results == 1:
                break
    (run_dir / "events.ndjson").write_text("\n".join(keep) + "\n")
    subprocess.run(["git", "reset", "--hard", "HEAD~1"], cwd=workdir, check=True,
                   capture_output=True)
    rig2 = _CountingRig("do")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig2})).run_epoch(
        tree, epoch=0
    )
    assert rig2.calls == 1  # the still-undone second position ran on resume


# --------------------------------------------------------------------------- NB3

def test_resume_after_converged_loop_iter_does_not_run_body_again(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    loop = Loop(body=Do(task="build", rig=RigRef(name="b")),
                until=Until(kind="cmd", cmd="true"), cap=5)  # converges immediately
    rig = _CountingRig("artifact")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"b": rig})).run_epoch(
        loop, epoch=0
    )
    assert rig.calls == 1
    # Crash after the converged loop_iter but before the loop's final result: TAIL-truncate
    # at the loop's final `result` (node n0), dropping it and the closing boundary while
    # keeping a contiguous prefix through the loop_iter (strict seq contiguity, hand-9).
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    cut = next(i for i, ln in enumerate(lines)
               if json.loads(ln)["kind"] == "result" and json.loads(ln)["node_id"] == "n0")
    (run_dir / "events.ndjson").write_text("\n".join(lines[:cut]) + "\n")

    rig2 = _CountingRig("artifact")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"b": rig2})).run_epoch(
        loop, epoch=0
    )
    assert rig2.calls == 0  # converged loop must NOT re-run its body
    final = replay(run_dir).results[(0, "n0")]
    assert "artifact.txt" in final.files
    assert final.ok is True


def test_resume_after_final_capped_loop_iter_preserves_last_body_artifact(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    loop = Loop(body=Do(task="build", rig=RigRef(name="b")),
                until=Until(kind="cmd", cmd="false"), cap=1)  # never converges, cap 1
    rig = _CountingRig("artifact")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"b": rig})).run_epoch(
        loop, epoch=0
    )
    assert rig.calls == 1
    # TAIL-truncate at the loop's final `result` (node n0): a crash after the final capped
    # loop_iter but before the loop result, keeping a contiguous prefix (hand-9).
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    cut = next(i for i, ln in enumerate(lines)
               if json.loads(ln)["kind"] == "result" and json.loads(ln)["node_id"] == "n0")
    (run_dir / "events.ndjson").write_text("\n".join(lines[:cut]) + "\n")

    rig2 = _CountingRig("artifact")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"b": rig2})).run_epoch(
        loop, epoch=0
    )
    assert rig2.calls == 0  # capped loop resumes straight to the result
    final = replay(run_dir).results[(0, "n0")]
    assert "artifact.txt" in final.files  # last body artifact preserved, not empty
    assert final.text == "artifact run 1"


# --------------------------------------------------------------------------- NB4

def test_resume_after_do_commit_before_result_does_not_repeat_or_lose_the_committed_effect(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    rig = _CountingRig("do")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        Do(task="work", rig=RigRef(name="c")), epoch=0
    )
    assert rig.calls == 1
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=workdir,
                          capture_output=True, text=True).stdout.strip()
    # Simulate a crash AFTER the core committed but BEFORE it journalled result/integrated:
    # keep the git commit, drop everything after `dispatched`.
    _truncate_before_kinds(run_dir, "result", "integrated")

    rig2 = _CountingRig("do")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig2})).run_epoch(
        Do(task="work", rig=RigRef(name="c")), epoch=0
    )
    # UPDATED to the hand-9 two-boundary contract (PROVENANCE-RANGE): a DISPATCHED-ONLY tail
    # has NO completion certificate (a mid-rig commit is not proof), so it is NEVER blessed
    # as success — the node RE-RUNS. The dead attempt's commit stays in history as forensic
    # residue (reachable, harmless), never reset away. The rerun writes the same content, so
    # the effect is not lost or doubled.
    assert rig2.calls == 1  # dispatched-only re-runs; the mid-rig commit is not a certificate
    assert (workdir / "do.txt").read_text() == "1"  # effect preserved (residue + rerun agree)


# --------------------------------------------------------------------------- NB5

def test_failed_do_cannot_leave_changes_for_a_later_do_to_integrate(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    reg = RigRegistry({
        "fail": ShellRig(template="printf leak > leak.txt; exit 3", timeout_s=30),
        "echo": EchoRig(),
    })
    tree = Seq(children=[
        Do(task="fail", rig=RigRef(name="fail")),   # n0.0 fails, leaves leak.txt
        Do(task="ok", rig=RigRef(name="echo")),      # n0.1 effectless success
    ])
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(tree, epoch=0)

    state = replay(run_dir)
    assert state.results[(0, "n0.0")].ok is False
    # The leak was captured to the run log dir and reset out of the workdir...
    assert (run_dir / "failed-diffs").exists()
    assert "captured" in state.results[(0, "n0.0")].text
    assert not (workdir / "leak.txt").exists()
    # ...so the later effectless do neither commits nor claims it.
    assert (0, "n0.1") not in state.integrated
    assert state.results[(0, "n0.1")].files == []
    assert "leak.txt" not in subprocess.run(
        ["git", "log", "--all", "--name-only"], cwd=workdir, capture_output=True, text=True
    ).stdout


def test_precommitted_script_rig_change_is_rejected_or_core_attributed(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    # A rig that commits its OWN work (the senior/script contract legitimately does).
    reg = _shell_reg(
        "printf y > authored.txt && git add authored.txt && git commit -q -m ownwork"
    )
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="author", rig=RigRef(name="shell")), epoch=0
    )
    state = replay(run_dir)
    # The core RECORDED the rig's own commit as this node's integration.
    assert (0, "n0") in state.integrated
    assert "authored.txt" in state.integrated[(0, "n0")]
    assert state.results[(0, "n0")].ok is True


# --------------------------------------------------------------------------- NB6

def test_file_ctx_rejects_absolute_parent_and_symlink_escapes(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    secret = tmp_path / "secret.txt"
    secret.write_text("HOST-SECRET", encoding="utf-8")
    reg = RigRegistry({"echo": EchoRig()})

    # (a) An absolute/`..` path escape is a LEXICAL error rejected at admission — now a
    # CtxRef construction-time validation error (item 5), before any epoch opens.
    with pytest.raises(ValidationError):
        CtxRef(kind="file", ref=str(secret))

    # (b) An in-worktree symlink pointing outside the workdir can only be judged at use
    # time (admission cannot resolve symlinks) -> a failed result, no exfiltration.
    os.symlink(secret, workdir / "link.txt")
    eng2 = Engine(run_dir=tmp_path / "run2", workdir=workdir, registry=reg)
    eng2.run_epoch(
        Do(task="y", rig=RigRef(name="echo"), ctx=[CtxRef(kind="file", ref="link.txt")]),
        epoch=0,
    )
    res2 = replay(tmp_path / "run2").results[(0, "n0")]
    assert res2.ok is False
    assert "HOST-SECRET" not in res2.text


# --------------------------------------------------------------------------- SF2

def test_unknown_rig_rejected_at_admission(tmp_path: Path) -> None:
    # An unknown rig name is a deterministic registry error the core rejects over the
    # whole tree BEFORE opening the epoch (item 5), not a runtime failed result.
    from wildflows.admission import AdmissionError

    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=RigRegistry({}))
    with pytest.raises(AdmissionError):
        eng.run_epoch(Do(task="x", rig=RigRef(name="typo")), epoch=0)
    assert not (tmp_path / "run" / "events.ndjson").exists()


# --------------------------------------------------------------------------- SF3

def test_shell_rig_timeout_kills_background_process_group(tmp_path: Path) -> None:
    pid_file = tmp_path / "pid"
    rig = ShellRig(
        template=f"sleep 30 & echo $! > {pid_file}; wait", timeout_s=0.4
    )
    result = rig.run("go", tmp_path)
    assert result.ok is False
    assert "timeout" in result.text
    time.sleep(0.2)
    child = int(pid_file.read_text().strip())
    with pytest.raises(ProcessLookupError):
        os.kill(child, 0)  # the backgrounded child was reaped with the group


# --------------------------------------------------------------------------- SF4

def test_resume_skips_nonempty_inplace_that_was_a_no_diff_noop(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    (workdir / "f.txt").write_text("same", encoding="utf-8")
    subprocess.run(["git", "add", "f.txt"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "seed"], cwd=workdir, check=True)

    tree = Inplace(edits=[Edit(path="f.txt", content="same")])  # already identical -> no diff
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({})).run_epoch(tree, epoch=0)
    state = replay(run_dir)
    assert state.results[(0, "n0")].ok is True  # a durable no-op, not a failure
    assert state.results[(0, "n0")].files == []

    # Crash before the epoch closed: drop ONLY the closing boundary (the last line),
    # resume -> the no-diff inplace must NOT be re-applied.
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    assert json.loads(lines[-1])["kind"] == "boundary"  # the closing boundary
    (run_dir / "events.ndjson").write_text("\n".join(lines[:-1]) + "\n")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({})).run_epoch(tree, epoch=0)
    dispatched = [e for e in _events(run_dir)
                  if e["kind"] == "dispatched" and e["node_id"] == "n0"]
    assert len(dispatched) == 1  # the no-diff inplace was durable, not re-dispatched


# --------------------------------------------------------------------------- SF5

def test_cmd_until_rejects_blank_or_whitespace_command() -> None:
    with pytest.raises(ValidationError):
        Until(kind="cmd", cmd="")
    with pytest.raises(ValidationError):
        Until(kind="cmd", cmd="   ")


# --------------------------------------------------------------------------- SF6

def test_do_integration_preserves_literal_whitespace_filename(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    reg = _shell_reg("printf x > 'two words.txt'")
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="make", rig=RigRef(name="shell")), epoch=0
    )
    state = replay(run_dir)
    assert state.integrated[(0, "n0")] == ["two words.txt"]  # one path, not split on space


# --------------------------------------------------------------------------- SF7

def test_shell_rig_nonzero_exit_has_failed_outcome(tmp_path: Path) -> None:
    result = ShellRig(template="exit 7", timeout_s=30).run("go", tmp_path)
    assert result.ok is False
    assert result.exit_code == 7
    assert result.outcome == "failed"  # not the default "ok"
