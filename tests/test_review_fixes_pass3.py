"""Regression tests from the external adversarial review, PASS 3 (hand-7).

Each carries the reviewer's exact name and exercises a NEW pass-3 finding fixed inside
the item-3/item-4 raze: reconciliation reachability, failed-rig committed/ignored leaks,
multi-commit attribution, and a ctx symlink alias to the git admin dir. A final test
proves the old-journal compatibility reader for the collapsed event shapes.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from wildflows.engine import Engine, replay
from wildflows.events import Boundary, parse_event
from wildflows.expr import CtxRef, Do, RigRef, assign_node_ids, parse_expr
from wildflows.journal import Journal
from wildflows.projection import RunProjection
from wildflows.rig import EchoRig, RigRegistry, ShellRig

from tests.test_review_fixes import _CountingRig, _git_init, _shell_reg


def _events(run_dir: Path) -> list[dict[str, object]]:
    return [json.loads(ln) for ln in (run_dir / "events.ndjson").read_text().splitlines()]


def _commit(workdir: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=workdir, check=True, capture_output=True)


# ------------------------------------------------------------------ BLOCKER 1

def test_reconciliation_requires_a_marked_commit_reachable_from_current_head(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "base.txt").write_text("base", encoding="utf-8")
    _commit(workdir, "add", "base.txt")
    _commit(workdir, "commit", "-qm", "base")
    base_branch = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=workdir, capture_output=True, text=True
    ).stdout.strip()

    # A marked commit for n0 exists — but only on a SIDE branch, absent from HEAD.
    run_dir = tmp_path / "run"
    marker = f"wf:{run_dir.name}:0:n0"
    _commit(workdir, "checkout", "-qb", "side")
    (workdir / "side.txt").write_text("side", encoding="utf-8")
    _commit(workdir, "add", "side.txt")
    _commit(workdir, "commit", "-qm", f"sidework\n\n{marker}")
    _commit(workdir, "checkout", "-q", base_branch)  # side.txt now absent from the worktree
    assert not (workdir / "side.txt").exists()

    # An epoch was opened for a `do` n0 that never resulted (in-flight on resume).
    tree = parse_expr(Do(task="work", rig=RigRef(name="c")).model_dump())
    assign_node_ids(tree)
    Journal(run_dir).append(
        Boundary(run_id=run_dir.name, epoch=0, node_id="n0", phase="opened", expr=tree.model_dump())
    )

    rig = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        Do(task="work", rig=RigRef(name="c")), epoch=0
    )
    # The unreachable marked commit must NOT be retro-integrated: the rig actually ran.
    assert rig.calls == 1
    state = replay(run_dir)
    assert "side.txt" not in state.integrated.get((0, "n0"), [])


# ------------------------------------------------------------------ BLOCKER 2

def test_failed_do_rig_commit_is_reverted_or_durably_attributed(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "base.txt").write_text("base", encoding="utf-8")
    _commit(workdir, "add", "base.txt")
    _commit(workdir, "commit", "-qm", "base")
    pre = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, capture_output=True, text=True
    ).stdout.strip()

    run_dir = tmp_path / "run"
    reg = RigRegistry({"fail": ShellRig(
        template="printf leaked > leak.txt; git add leak.txt; git commit -qm rig-commit; exit 7",
        timeout_s=30,
    )})
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="leak", rig=RigRef(name="fail")), epoch=0
    )

    head_after = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, capture_output=True, text=True
    ).stdout.strip()
    assert head_after == pre  # the failing rig's own commit was reverted to pre-HEAD
    assert not (workdir / "leak.txt").exists()
    state = replay(run_dir)
    assert state.results[(0, "n0")].ok is False
    assert (0, "n0") not in state.integrated  # the reverted effect is not attributed
    assert "captured" in state.results[(0, "n0")].text
    # the reverted commit's content survives as failure evidence
    leaked = (run_dir / "failed-diffs" / "e0-n0.diff").read_text()
    assert "leaked" in leaked


# --------------------------------------------------------------------- HIGH 3

def test_failed_do_cleanup_captures_and_removes_ignored_artifacts(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / ".gitignore").write_text("*.leak\n", encoding="utf-8")
    _commit(workdir, "add", ".gitignore")
    _commit(workdir, "commit", "-qm", "ignore")

    run_dir = tmp_path / "run"
    reg = RigRegistry({"fail": ShellRig(
        template="printf secret > keep.leak; exit 4", timeout_s=30)})
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="leak", rig=RigRef(name="fail")), epoch=0
    )

    assert not (workdir / "keep.leak").exists()  # the ignored artifact was removed (clean -x)
    evidence = (run_dir / "failed-diffs" / "e0-n0.diff").read_text()
    assert "keep.leak" in evidence and "secret" in evidence  # ...AND captured first


# ------------------------------------------------------------------ BLOCKER 4

def test_rig_multi_commit_attribution_records_every_commit_or_a_verifiable_range(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    # A rig that authors TWO commits (a then b) in an unborn repo.
    reg = _shell_reg(
        "printf a > a.txt && git add a.txt && git commit -qm one && "
        "printf b > b.txt && git add b.txt && git commit -qm two"
    )
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="author", rig=RigRef(name="shell")), epoch=0
    )

    state = replay(run_dir)
    paths = state.integrated[(0, "n0")]
    assert set(paths) == {"a.txt", "b.txt"}  # EVERY commit's paths attributed, not just the last
    receipt = state.receipts[(0, "n0")]
    assert len(receipt.commits) == 2  # both commits recorded verifiably
    # every recorded sha is a real reachable commit
    for sha in receipt.shas:
        rc = subprocess.run(["git", "cat-file", "-e", sha], cwd=workdir).returncode
        assert rc == 0


# --------------------------------------------------------------------- HIGH 5

def test_file_ctx_rejects_a_symlink_alias_to_the_git_admin_dir(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "seed.txt").write_text("seed", encoding="utf-8")
    _commit(workdir, "add", "seed.txt")
    _commit(workdir, "commit", "-qm", "seed")
    # An in-worktree alias to the git admin dir; its config carries secrets in the real world.
    os.symlink(workdir / ".git", workdir / "administration")

    run_dir = tmp_path / "run"
    eng = Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"echo": EchoRig()}))
    eng.run_epoch(
        Do(task="read", rig=RigRef(name="echo"),
           ctx=[CtxRef(kind="file", ref="administration/config")]),
        epoch=0,
    )
    res = replay(run_dir).results[(0, "n0")]
    assert res.ok is False  # the gitdir-alias read is a failed result, not an exfiltration
    assert "[core]" not in res.text and "repositoryformatversion" not in res.text


# --------------------------------------------------- old-journal compatibility

def test_compatibility_reader_folds_legacy_result_and_integrated_shapes() -> None:
    """A pre-collapse journal (a bare `ok=False` result with no `outcome`, and a
    single-commit `integrated` with `commit`/`paths`) folds into the current projection
    via the compatibility readers — no re-generation of old runs required."""
    proj = RunProjection()
    legacy_result = {
        "seq": 0, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "result",
        "ok": False, "text": "boom", "files": ["x.txt"],  # NO outcome field
    }
    legacy_integrated = {
        "seq": 1, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "integrated",
        "commit": "deadbeef", "paths": ["x.txt"],  # old single-commit shape
    }
    proj.apply(parse_event(legacy_result))
    proj.apply(parse_event(legacy_integrated))

    res = proj.results[(0, "n0")]
    assert res.outcome == "failed" and res.ok is False  # ok=False migrated to outcome
    receipt = proj.receipts[(0, "n0")]
    assert receipt.shas == ["deadbeef"] and receipt.paths == ["x.txt"]


def test_dispatched_only_reachable_commit_reruns_and_is_not_reconciled(tmp_path: Path) -> None:
    """UPDATED to the hand-9 two-boundary contract (PROVENANCE-RANGE): a DISPATCHED-ONLY
    tail — even with a reachable marked commit — has no completion certificate, so recovery
    never blesses it as success. The node re-runs; the mid-rig commit is forensic residue."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    run_dir = tmp_path / "run"
    rig = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        Do(task="work", rig=RigRef(name="c")), epoch=0
    )
    assert rig.calls == 1
    # Drop everything after `dispatched` (result + integrated) — the marked commit stays on HEAD.
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    cut = next(i for i, ln in enumerate(lines) if json.loads(ln)["kind"] == "result")
    (run_dir / "events.ndjson").write_text("\n".join(lines[:cut]) + "\n")

    rig2 = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig2})).run_epoch(
        Do(task="work", rig=RigRef(name="c")), epoch=0
    )
    assert rig2.calls == 1  # no completion proof from a dispatched-only tail — it re-runs
    assert (workdir / "c.txt").read_text() == "1"  # effect preserved (residue + rerun agree)
