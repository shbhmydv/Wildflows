"""Regression tests from the external adversarial review, PASS 5 (hand-9).

Each carries the reviewer's exact name (adapted where the CTO triage changed the contract).
They exercise the pass-5 findings: the two-boundary provenance range (`pre_head..post_head`,
dispatched-only = always rerun), the clean-worktree failure lease (dirty-tracked refusal,
per-file untracked granularity, binary-tolerant capture), loop outcome totality + the
nested-loop resume floor, transactional inplace, strict seq contiguity + receipt SHA
validation, and result-producing ctx-ref admission.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from wildflows.admission import AdmissionError
from wildflows.engine import Engine, replay
from wildflows.events import Boundary, Dispatched, parse_event
from wildflows.expr import CtxRef, Dispatch, Do, Edit, Inplace, Loop, RigRef, Seq, Until
from wildflows.journal import Journal, JournalCompatibilityError
from wildflows.rig import EchoRig, RigRegistry

from tests.test_review_fixes import _CountingRig, _git_init, _shell_reg
from tests.test_review_fixes_pass4 import _head, parse_expr_dump


def _base_repo(workdir: Path) -> str:
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "base.txt").write_text("base", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=workdir, check=True)
    return _head(workdir)


def _commit_file(workdir: Path, name: str, content: str, msg: str) -> None:
    (workdir / name).write_text(content, encoding="utf-8")
    subprocess.run(["git", "add", name], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", msg], cwd=workdir, check=True)


# ------------------------------------------------------------ BLOCKER 1: PROVENANCE-RANGE

def test_dispatched_with_mid_rig_commit_is_not_recovered_as_success(tmp_path: Path) -> None:
    """A dispatched-only tail with a mid-rig checkpoint commit has NO completion certificate,
    so it is never synthesized as success — the node RE-RUNS. Under hand-10 QUARANTINE-NEVER-
    DESTROY the checkpoint commit is moved to a quarantine ref (reachable forensic residue)
    and the branch is reset to pre_head, so the rerun starts clean and must own its own
    receipt (it can never absorb the retained commit as unreceipted success)."""
    workdir = tmp_path / "work"
    pre = _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="c"))

    j = Journal(run_dir)
    j.append(Boundary(run_id=run_dir.name, epoch=0, node_id="n0", phase="opened",
                      expr=parse_expr_dump(tree)))
    j.append(Dispatched(run_id=run_dir.name, epoch=0, node_id="n0", rig="c",
                        task="work", pre_head=pre))
    # A mid-rig checkpoint commit, then death before the rig returned (no result event).
    _commit_file(workdir, "chk.txt", "checkpoint", f"chk\n\nwf:{run_dir.name}:0:n0")
    chk_sha = _head(workdir)

    rig = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        tree, epoch=0
    )
    assert rig.calls == 1  # re-ran; a checkpoint commit is not a completion certificate
    assert not (workdir / "chk.txt").exists()  # branch reset to pre_head; not at HEAD
    # The checkpoint commit is preserved (reachable) in a quarantine ref — never destroyed.
    quarantined = subprocess.run(
        ["git", "for-each-ref", "--format=%(objectname)", "refs/wildflows/quarantine/"],
        cwd=workdir, capture_output=True, text=True).stdout.split()
    assert chk_sha in quarantined  # dead-attempt commit is safe, not lost
    # The rerun owns its OWN active effect via a receipt — no unreceipted retained commit.
    assert replay(run_dir).integrated[(0, "n0")]  # receipt present for the successful rerun


def test_unborn_mid_rig_commit_is_not_completion(tmp_path: Path) -> None:
    """The same on an UNBORN repo (pre_head=None): a first mid-rig commit with no result is
    not a completion — the node re-runs."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="c"))

    j = Journal(run_dir)
    j.append(Boundary(run_id=run_dir.name, epoch=0, node_id="n0", phase="opened",
                      expr=parse_expr_dump(tree)))
    j.append(Dispatched(run_id=run_dir.name, epoch=0, node_id="n0", rig="c",
                        task="work", pre_head=None))
    _commit_file(workdir, "first.txt", "x", f"first\n\nwf:{run_dir.name}:0:n0")

    rig = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        tree, epoch=0
    )
    assert rig.calls == 1  # unborn mid-rig commit is not completion


def test_result_integrated_tear_rejects_post_attempt_operator_commit(tmp_path: Path) -> None:
    """A result-then-integrated tear reconstructs the receipt from EXACTLY
    `pre_head..post_head`. An operator commit made after process death (above post_head) is
    outside the range by construction and is NEVER attributed to the attempt."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    Engine(run_dir=run_dir, workdir=workdir,
           registry=_shell_reg("printf a>a.txt && git add a.txt && git commit -qm one")
           ).run_epoch(Do(task="author", rig=RigRef(name="shell")), epoch=0)
    assert replay(run_dir).integrated[(0, "n0")] == ["a.txt"]

    # Crash after `result` (post_head stamped) but before `integrated`: drop integrated+close.
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    cut = next(i for i, ln in enumerate(lines) if json.loads(ln)["kind"] == "integrated")
    (run_dir / "events.ndjson").write_text("\n".join(lines[:cut]) + "\n")
    # An operator hotfix lands ABOVE post_head after death.
    _commit_file(workdir, "op.txt", "hotfix", "operator hotfix")

    Engine(run_dir=run_dir, workdir=workdir, registry=_shell_reg("exit 99")).run_epoch(
        Do(task="author", rig=RigRef(name="shell")), epoch=0
    )
    integrated = replay(run_dir).integrated[(0, "n0")]
    assert integrated == ["a.txt"]  # ONLY the attempt's commit — never op.txt


def test_operator_commit_after_crash_is_not_attributed_to_attempt(tmp_path: Path) -> None:
    """A dispatched-only crash followed by an operator commit: the node re-runs, and the
    operator commit (below the rerun's fresh pre_head) is never attributed to the node."""
    workdir = tmp_path / "work"
    pre = _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="c"))
    j = Journal(run_dir)
    j.append(Boundary(run_id=run_dir.name, epoch=0, node_id="n0", phase="opened",
                      expr=parse_expr_dump(tree)))
    j.append(Dispatched(run_id=run_dir.name, epoch=0, node_id="n0", rig="c",
                        task="work", pre_head=pre))
    _commit_file(workdir, "op.txt", "hotfix", "operator hotfix after crash")

    rig = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        tree, epoch=0
    )
    assert rig.calls == 1  # re-ran; nothing recovered as success
    assert "op.txt" not in replay(run_dir).integrated.get((0, "n0"), [])


# --------------------------------------------------------------- BLOCKER 2: FAILURE-LEASE

def test_failed_attempt_preserves_preexisting_tracked_and_staged_changes(tmp_path: Path) -> None:
    """The lease REFUSES to open on a dirty tracked/index worktree — a durable failed
    result — so a pre-existing user tracked modification is never destroyed by a revert."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "base.txt").write_text("USER MODIFICATION BEFORE LEASE", encoding="utf-8")

    run_dir = tmp_path / "run"
    reg = _shell_reg("printf boom > leak.txt; exit 5")
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="leak", rig=RigRef(name="shell")), epoch=0
    )

    assert (workdir / "base.txt").read_text() == "USER MODIFICATION BEFORE LEASE"  # preserved
    assert not (workdir / "leak.txt").exists()  # the rig never ran (lease refused)
    state = replay(run_dir)
    assert state.results[(0, "n0")].ok is False
    assert "uncommitted tracked changes" in state.results[(0, "n0")].text


def test_failed_attempt_removes_new_child_of_preexisting_untracked_directory(
    tmp_path: Path,
) -> None:
    """Per-file untracked snapshot (`-uall`): a failed attempt's addition UNDER a
    pre-existing untracked directory is detected and swept, while the pre-existing sibling
    survives."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "scratch").mkdir()
    (workdir / "scratch" / "keep").write_text("keep", encoding="utf-8")  # pre-existing

    run_dir = tmp_path / "run"
    reg = _shell_reg("printf leak > scratch/leak; exit 5")
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="leak", rig=RigRef(name="shell")), epoch=0
    )

    assert not (workdir / "scratch" / "leak").exists()          # the new child was swept
    assert (workdir / "scratch" / "keep").read_text() == "keep"  # pre-existing sibling kept
    assert replay(run_dir).results[(0, "n0")].ok is False


def test_failed_attempt_binary_artifact_is_captured_and_removed_without_escape(
    tmp_path: Path,
) -> None:
    """A failed rig leaving a BINARY untracked artifact is captured (size + sha256) and
    removed — no decode exception escapes after `dispatched`."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    # Bytes 0xFF 0xFE 0xFD are invalid UTF-8; `read_text` would raise without the guard.
    reg = _shell_reg("printf '\\377\\376\\375' > bin.dat; exit 7")
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(  # must not raise
        Do(task="bin", rig=RigRef(name="shell")), epoch=0
    )

    assert not (workdir / "bin.dat").exists()  # binary leak removed
    evidence = (run_dir / "failed-diffs" / "e0-n0.diff").read_text()
    assert "binary artifact" in evidence and "sha256=" in evidence
    assert replay(run_dir).results[(0, "n0")].ok is False


# ---------------------------------------------------------- BLOCKER 3: LOOP-OUTCOME-TOTALITY

def test_loop_rejects_trailing_empty_composite_outcome(tmp_path: Path) -> None:
    """Admission rejects a composite whose last positional child chain does not terminate in
    a result-producing leaf — making `result_key()` total by construction."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=RigRegistry({"c": EchoRig()}))
    loop = Loop(
        body=Seq(children=[Do(task="artifact", rig=RigRef(name="c")), Seq(children=[])]),
        until=Until(kind="cmd", cmd="true"), cap=1,
    )
    with pytest.raises(AdmissionError, match="result-producing"):
        eng.run_epoch(loop, epoch=0)
    assert not (tmp_path / "run" / "events.ndjson").exists()


def test_uninterrupted_and_resumed_loop_use_same_explicit_body_reference(tmp_path: Path) -> None:
    """An uninterrupted loop and a crash-after-loop_iter resume produce the SAME final
    artifact — both resolved through the explicit `body_result_seq` reference."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    loop = Loop(body=Seq(children=[Do(task="build", rig=RigRef(name="b"))]),
                until=Until(kind="cmd", cmd="true"), cap=3)
    Engine(run_dir=run_dir, workdir=workdir,
           registry=RigRegistry({"b": _CountingRig("art")})).run_epoch(loop, epoch=0)
    uninterrupted = replay(run_dir).results[(0, "n0")]
    assert "art.txt" in uninterrupted.files and uninterrupted.text == "art run 1"

    lines = (run_dir / "events.ndjson").read_text().splitlines()
    cut = next(i for i, ln in enumerate(lines)
               if json.loads(ln)["kind"] == "result" and json.loads(ln)["node_id"] == "n0")
    (run_dir / "events.ndjson").write_text("\n".join(lines[:cut]) + "\n")

    rig2 = _CountingRig("art")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"b": rig2})).run_epoch(
        loop, epoch=0
    )
    assert rig2.calls == 0  # converged loop reconstructs, does not re-run
    resumed = replay(run_dir).results[(0, "n0")]
    assert resumed.files == uninterrupted.files and resumed.text == uninterrupted.text


def test_nested_loop_fresh_outer_iteration_reruns_inner_body(tmp_path: Path) -> None:
    """Each FRESH outer iteration re-runs the inner loop's body: it must not treat a prior
    outer iteration's inner result as durable (the nested-loop floor bug)."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    inner = Loop(body=Do(task="tick", rig=RigRef(name="c")),
                 until=Until(kind="cmd", cmd="false"), cap=1)  # never converges: 1 body/iter
    outer = Loop(body=inner, until=Until(kind="cmd", cmd="false"), cap=2)  # 2 outer iters
    rig = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        outer, epoch=0
    )
    assert rig.calls == 2  # inner body ran once per fresh outer iteration


def test_nested_loop_resume_floor_does_not_reuse_prior_outer_iteration(tmp_path: Path) -> None:
    """Truncating after the first outer `loop_iter` and resuming re-runs the inner body for
    the next outer iteration — a prior outer iteration's inner iters are out of floor scope."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    inner = Loop(body=Do(task="tick", rig=RigRef(name="c")),
                 until=Until(kind="cmd", cmd="false"), cap=1)
    outer = Loop(body=inner, until=Until(kind="cmd", cmd="false"), cap=2)
    rig = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        outer, epoch=0
    )
    assert rig.calls == 2

    # Truncate just AFTER the first OUTER loop_iter (node n0): outer iteration 0 is complete,
    # iteration 1 has not started.
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    cut = next(i for i, ln in enumerate(lines)
               if json.loads(ln)["kind"] == "loop_iter" and json.loads(ln)["node_id"] == "n0")
    (run_dir / "events.ndjson").write_text("\n".join(lines[:cut + 1]) + "\n")

    rig2 = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig2})).run_epoch(
        outer, epoch=0
    )
    assert rig2.calls == 1  # outer iteration 1's inner body re-ran, not reused


# ------------------------------------------------------ BLOCKER 4: INPLACE-TRANSACTIONAL

def test_inplace_late_symlink_rejection_rolls_back_earlier_edits(tmp_path: Path) -> None:
    """An inplace whose FIRST edit writes and whose SECOND escapes via a symlink rolls back
    the first write — a durable failed result leaves NO partial effect."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    outside = tmp_path / "outside"
    outside.mkdir()
    os.symlink(outside, workdir / "link")

    run_dir = tmp_path / "run"
    eng = Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({}))
    eng.run_epoch(Inplace(edits=[
        Edit(path="safe.txt", content="safe"),        # written first
        Edit(path="link/pwn.txt", content="x"),        # late symlink escape -> reject
    ]), epoch=0)

    assert not (workdir / "safe.txt").exists()   # earlier write rolled back
    assert not (outside / "pwn.txt").exists()    # nothing written outside
    state = replay(run_dir)
    assert state.results[(0, "n0")].ok is False
    assert (0, "n0") not in state.integrated


def test_inplace_commit_failure_reverts_all_declared_writes(tmp_path: Path) -> None:
    """When the declared commit fails (no git identity), every write is reverted and
    unstaged — no partial effect survives the durable failed result."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    env = {"GIT_CONFIG_GLOBAL": str(tmp_path / "no-g"), "GIT_CONFIG_SYSTEM": str(tmp_path / "no-s")}
    subprocess.run(["git", "init", "-q"], cwd=workdir, check=True, env={**os.environ, **env})
    for var in ("GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME",
                "GIT_COMMITTER_EMAIL"):
        os.environ.pop(var, None)
    os.environ.update(env)
    run_dir = tmp_path / "run"
    try:
        Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({})).run_epoch(
            Inplace(edits=[Edit(path="a.txt", content="a"), Edit(path="b.txt", content="b")]),
            epoch=0,
        )
    finally:
        for k in env:
            os.environ.pop(k, None)

    assert not (workdir / "a.txt").exists() and not (workdir / "b.txt").exists()  # reverted
    staged = subprocess.run(["git", "diff", "--cached", "--name-only"], cwd=workdir,
                            capture_output=True, text=True, env={**os.environ, **env}).stdout
    assert staged.strip() == ""  # nothing left staged
    assert replay(run_dir).results[(0, "n0")].ok is False


# ----------------------------------------------------------------- HIGH 5: SEQ+RECEIPT

def test_load_rejects_middle_sequence_gap_with_missing_integrated(tmp_path: Path) -> None:
    """A middle gap (a missing `integrated` at seq 3) is refused — a genuine torn tail can
    never create a middle gap, so a gap can only hide a lost durability event."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    stream = [
        {"seq": 0, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "opened"},
        {"seq": 1, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "dispatched",
         "rig": "c", "task": "t", "pre_head": None},
        {"seq": 2, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "result",
         "text": "x", "files": ["x.txt"], "outcome": "ok"},
        # seq 3 (the integrated) is ABSENT — a middle gap.
        {"seq": 4, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "closed"},
    ]
    (run_dir / "events.ndjson").write_text(
        "\n".join(json.dumps(r) for r in stream) + "\n", encoding="utf-8"
    )
    with pytest.raises(JournalCompatibilityError, match="not contiguous"):
        Journal.load(run_dir)


def test_load_rejects_negative_assigned_sequence(tmp_path: Path) -> None:
    """A negative (unassigned) seq on disk is rejected — every appended event carries an
    assigned seq >= 0."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    stream = [
        {"seq": -1, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "opened"},
    ]
    (run_dir / "events.ndjson").write_text(
        "\n".join(json.dumps(r) for r in stream) + "\n", encoding="utf-8"
    )
    with pytest.raises(JournalCompatibilityError, match="not contiguous"):
        Journal.load(run_dir)


def test_integrated_rejects_empty_commit_sha() -> None:
    """A receipt with a blank commit sha (modern or migrated-legacy) is rejected — an empty
    sha proves no effect and must not mark a node durable."""
    with pytest.raises(ValidationError):
        parse_event({"run_id": "r", "epoch": 0, "node_id": "n0", "kind": "integrated",
                     "commits": [{"sha": "", "paths": ["x"]}]})
    with pytest.raises(ValidationError):
        parse_event({"run_id": "r", "epoch": 0, "node_id": "n0", "kind": "integrated",
                     "commit": "", "paths": ["x"]})  # legacy blank commit


# --------------------------------------------------- HIGH 6: ADMISSION-REF-RESULTFUL

def test_admission_rejects_ctx_ref_to_seq_or_dispatch_node(tmp_path: Path) -> None:
    """A ctx ref to a structural `seq`/`dispatch` node (which journals no result) is rejected
    at admission, not left to fail as an unresolved ctx at run time."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    reg = RigRegistry({"c": EchoRig()})

    # n0.0 is an inner Seq (no node-level result); n0.1 refs it.
    with pytest.raises(AdmissionError, match="produces no result"):
        Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg).run_epoch(
            Seq(children=[
                Seq(children=[Do(task="a", rig=RigRef(name="c"))]),
                Do(task="b", rig=RigRef(name="c"), ctx=[CtxRef(kind="node", ref="n0.0")]),
            ]), epoch=0)

    # A Dispatch target is equally resultless.
    with pytest.raises(AdmissionError, match="produces no result|crosses a Dispatch"):
        Engine(run_dir=tmp_path / "run2", workdir=workdir, registry=reg).run_epoch(
            Seq(children=[
                Dispatch(children=[Do(task="a", rig=RigRef(name="c"))]),
                Do(task="b", rig=RigRef(name="c"), ctx=[CtxRef(kind="node", ref="n0.0")]),
            ]), epoch=1)


def test_admission_rejects_ctx_ref_to_unfinished_ancestor_composite(tmp_path: Path) -> None:
    """A ctx ref to an enclosing ANCESTOR composite is rejected — its result cannot exist
    before the consumer inside it runs."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    reg = RigRegistry({"c": EchoRig()})
    # n0 is the enclosing Seq ancestor of the consumer n0.0.
    with pytest.raises(AdmissionError, match="ancestor"):
        Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg).run_epoch(
            Seq(children=[
                Do(task="b", rig=RigRef(name="c"), ctx=[CtxRef(kind="node", ref="n0")]),
            ]), epoch=0)
