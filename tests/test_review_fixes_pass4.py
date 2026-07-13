"""Regression tests from the external adversarial review, PASS 4 (hand-8).

Each carries the reviewer's exact name (adapted where the triage changed the contract to
a refusal). They exercise the pass-4 findings: provenance-based receipt recovery, the
lease-scoped failure transaction, the explicit loop body-outcome reference, pre-v1 legacy
journal refusal, reverted-effect non-reconciliation, malformed-receipt rejection, and
upstream ctx-ref admission.
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
from wildflows.projection import RunProjection
from wildflows.rig import EchoRig, RigRegistry, ShellRig

from tests.test_review_fixes import _CountingRig, _git_init, _shell_reg


def _head(workdir: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, capture_output=True, text=True
    ).stdout.strip()


def _n_commits(workdir: Path) -> int:
    out = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"], cwd=workdir, capture_output=True, text=True
    ).stdout.strip()
    return int(out) if out else 0


def _drop_from_first_kind(run_dir: Path, kind: str) -> None:
    """Truncate the journal at (excluding) the first event of `kind` — simulate a crash
    in the completion gap that left git committed but the journal tail unwritten."""
    lines = (run_dir / "events.ndjson").read_text().splitlines()
    cut = next(i for i, ln in enumerate(lines) if json.loads(ln)["kind"] == kind)
    (run_dir / "events.ndjson").write_text("\n".join(lines[:cut]) + "\n")


# ---------------------------------------------------------------- BLOCKER 1

def test_resume_after_result_before_integrated_preserves_full_rig_receipt(
    tmp_path: Path,
) -> None:
    """A crash after `result` but before `integrated` (the recorder gap) must NOT lose
    the rig's own commits nor re-run: the attempt's `pre_head..HEAD` range reconstructs
    the FULL receipt (pure-rig and mixed rig+core commit cases)."""
    for name, template, expected in (
        ("pure", "printf a>a.txt && git add a.txt && git commit -qm one && "
                 "printf b>b.txt && git add b.txt && git commit -qm two", {"a.txt", "b.txt"}),
        ("mixed", "printf a>a.txt && git add a.txt && git commit -qm one && "
                  "printf b>b.txt", {"a.txt", "b.txt"}),
    ):
        workdir = tmp_path / f"work-{name}"
        workdir.mkdir()
        _git_init(workdir)
        (workdir / "base.txt").write_text("base", encoding="utf-8")
        subprocess.run(["git", "add", "base.txt"], cwd=workdir, check=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=workdir, check=True)
        run_dir = tmp_path / f"run-{name}"
        Engine(run_dir=run_dir, workdir=workdir, registry=_shell_reg(template)).run_epoch(
            Do(task="author", rig=RigRef(name="shell")), epoch=0
        )
        head, ncommits = _head(workdir), _n_commits(workdir)
        assert set(replay(run_dir).integrated[(0, "n0")]) == expected

        # Crash in the recorder gap: drop `integrated` (and the close) — result stays.
        _drop_from_first_kind(run_dir, "integrated")

        Engine(run_dir=run_dir, workdir=workdir, registry=_shell_reg("exit 99")).run_epoch(
            Do(task="author", rig=RigRef(name="shell")), epoch=0
        )
        assert _head(workdir) == head and _n_commits(workdir) == ncommits  # not re-run
        state = replay(run_dir)
        assert set(state.integrated[(0, "n0")]) == expected  # full receipt preserved


# ---------------------------------------------------------------- BLOCKER 2

def test_do_integration_failure_reverts_and_captures_dirty_state(tmp_path: Path) -> None:
    """A successful rig whose dirty state the CORE fails to commit routes through the
    failure transaction (revert + capture), never a bare error that leaks the change."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    # A repo with NO commit identity, so the core's integration commit fails.
    monkeypatch_env = {
        "GIT_CONFIG_GLOBAL": str(tmp_path / "no-global"),
        "GIT_CONFIG_SYSTEM": str(tmp_path / "no-system"),
    }
    subprocess.run(["git", "init", "-q"], cwd=workdir, check=True,
                   env={**os.environ, **monkeypatch_env})
    run_dir = tmp_path / "run"
    # A rig that succeeds and leaves a dirty (uncommitted) file for the core to integrate.
    reg = RigRegistry({
        "shell": ShellRig(template="printf leaked > leak.txt", timeout_s=30),
        "echo": EchoRig(),
    })
    tree = Seq(children=[
        Do(task="leak", rig=RigRef(name="shell")),     # n0.0: core integration fails
        Do(task="ok", rig=RigRef(name="echo")),          # n0.1: effectless success
    ])
    for var in ("GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME",
                "GIT_COMMITTER_EMAIL"):
        os.environ.pop(var, None)
    os.environ.update(monkeypatch_env)
    try:
        Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(tree, epoch=0)
    finally:
        for k in monkeypatch_env:
            os.environ.pop(k, None)

    state = replay(run_dir)
    assert state.results[(0, "n0.0")].ok is False
    assert not (workdir / "leak.txt").exists()          # reverted out of the workdir
    assert "captured" in state.results[(0, "n0.0")].text  # evidence captured
    assert (0, "n0.1") not in state.integrated          # later do never inherits the leak
    assert state.results[(0, "n0.1")].files == []


def test_failure_cleanup_preserves_run_dir_and_preexisting_untracked_files(
    tmp_path: Path,
) -> None:
    """Failure cleanup is LEASE-SCOPED: it removes only THIS attempt's leaks, never the
    run_dir (even inside the workdir) nor pre-existing untracked user files."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "base.txt").write_text("base", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=workdir, check=True)
    # A pre-existing untracked user file present BEFORE the attempt opens.
    (workdir / "user_notes.txt").write_text("keep me", encoding="utf-8")

    run_dir = workdir / ".wildflows" / "run"  # the documented target-repo location: INSIDE
    reg = _shell_reg("printf boom > leak.txt; exit 5")
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="leak", rig=RigRef(name="shell")), epoch=0
    )

    assert not (workdir / "leak.txt").exists()               # the leak was swept
    assert (workdir / "user_notes.txt").read_text() == "keep me"  # user file preserved
    assert (run_dir / "events.ndjson").exists()              # the journal survived cleanup
    state = replay(run_dir)
    assert state.results[(0, "n0")].ok is False


def test_failed_do_cleanup_removes_and_captures_nested_git_repository(tmp_path: Path) -> None:
    """A failed rig's nested Git repo is CAPTURED (its file listing, not `<unreadable>`)
    and removed — a plain `git clean -fd` refuses a nested repo."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "base.txt").write_text("base", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=workdir, check=True)

    run_dir = tmp_path / "run"
    reg = _shell_reg(
        "mkdir nest && git init -q nest && printf topsecret > nest/secret && exit 6"
    )
    Engine(run_dir=run_dir, workdir=workdir, registry=reg).run_epoch(
        Do(task="nest", rig=RigRef(name="shell")), epoch=0
    )

    assert not (workdir / "nest").exists()  # nested repo removed recursively
    evidence = (run_dir / "failed-diffs" / "e0-n0.diff").read_text()
    assert "nest/secret" in evidence and "topsecret" in evidence  # listing + content
    assert "<binary or unreadable>" not in evidence.split("nest/secret")[1][:60]


# ---------------------------------------------------------------- BLOCKER 3

def test_loop_iter_persists_positioned_body_reference_under_out_of_order_dispatch_completion() -> None:
    """`loop_iter` folds its body artifact through its EXPLICIT `body_result_seq`, not the
    process-global last-folded result. Under an out-of-order Dispatch completion (body B
    is the last input position but A folds last), the loop body outcome must be B."""
    proj = RunProjection()
    events = [
        {"seq": 0, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "opened"},
        # Body B (the loop body's declared last position) completes/folds FIRST (seq 1)...
        {"seq": 1, "run_id": "r", "epoch": 0, "node_id": "n0.0.1", "kind": "result",
         "text": "B artifact", "files": ["b.txt"], "outcome": "ok"},
        # ...then sibling A folds LAST (seq 2), so the global last-result is A.
        {"seq": 2, "run_id": "r", "epoch": 0, "node_id": "n0.0.0", "kind": "result",
         "text": "A artifact", "files": ["a.txt"], "outcome": "ok"},
        # loop_iter references B explicitly (seq 1), not the last-folded A.
        {"seq": 3, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "loop_iter",
         "iteration": 0, "converged": True, "body_result_seq": 1},
    ]
    for ev in events:
        proj.apply(parse_event(ev))

    body = proj.node((0, "n0")).loop_last_body
    assert body is not None
    assert body.text == "B artifact" and body.files == ["b.txt"]  # referenced, not last-folded


def test_empty_composite_loop_body_is_rejected_at_admission(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=RigRegistry({}))
    loop = Loop(body=Seq(children=[]), until=Until(kind="cmd", cmd="true"), cap=1)
    with pytest.raises(AdmissionError, match="no executable leaf"):
        eng.run_epoch(loop, epoch=0)
    assert not (tmp_path / "run" / "events.ndjson").exists()


# ---------------------------------------------------------------- BLOCKER 4

def test_legacy_integrated_before_result_partial_loop_tail_is_not_reexecuted(
    tmp_path: Path,
) -> None:
    """ADAPTED to the refusal contract: a pre-v1 legacy journal with an INTERRUPTED tail
    (a legacy-shape record after the last boundary, here a `dispatched` with no
    `pre_head`) cannot be provenance-resumed — load raises rather than re-running and
    duplicating the effect."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    legacy = [
        # a complete first iteration, then an interrupted second iteration tail
        {"seq": 0, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "opened"},
        {"seq": 1, "run_id": "r", "epoch": 0, "node_id": "n0.0", "kind": "dispatched",
         "rig": "c", "task": "tick"},  # LEGACY: no pre_head field
        {"seq": 2, "run_id": "r", "epoch": 0, "node_id": "n0.0", "kind": "result",
         "ok": True, "text": "tick 1", "files": ["t.txt"], "outcome": "ok"},
        {"seq": 3, "run_id": "r", "epoch": 0, "node_id": "n0.0", "kind": "integrated",
         "commit": "abc", "paths": ["t.txt"]},  # LEGACY single-commit shape
        {"seq": 4, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "loop_iter",
         "iteration": 0, "commit": "abc", "converged": False,
         "body_text": "tick 1", "body_files": ["t.txt"], "body_exit_code": 0},  # LEGACY
        # interrupted second iteration: a legacy dispatched, no result — cannot recover
        {"seq": 5, "run_id": "r", "epoch": 0, "node_id": "n0.0", "kind": "dispatched",
         "rig": "c", "task": "tick"},
    ]
    (run_dir / "events.ndjson").write_text(
        "\n".join(json.dumps(r) for r in legacy) + "\n", encoding="utf-8"
    )
    with pytest.raises(JournalCompatibilityError, match="legacy journal tail"):
        Journal.load(run_dir)


# ------------------------------------------------------------------- HIGH 5

def test_reconciliation_rejects_a_reachable_marker_whose_effect_was_reverted(
    tmp_path: Path,
) -> None:
    """Recovery keys off THIS attempt's `pre_head..HEAD` range, not an ancestor marker
    scan: a marked effect that a PRIOR attempt already reverted is outside the range, so
    it is NOT falsely reconciled — the rig re-runs and the reverted file is not attributed."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "base.txt").write_text("base", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=workdir, check=True)

    run_dir = tmp_path / "run"
    marker = f"wf:{run_dir.name}:0:n0"
    # A prior attempt's marked effect, then its revert — BOTH now ancestors of HEAD.
    (workdir / "lost.txt").write_text("gone", encoding="utf-8")
    subprocess.run(["git", "add", "lost.txt"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", f"add lost\n\n{marker}"], cwd=workdir, check=True)
    (workdir / "lost.txt").unlink()
    subprocess.run(["git", "commit", "-aqm", "revert lost"], cwd=workdir, check=True)
    assert not (workdir / "lost.txt").exists()
    pre = _head(workdir)  # this attempt opens AFTER the revert

    # An in-flight `do` n0 dispatched with pre_head at the post-revert HEAD, no result.
    tree = Do(task="work", rig=RigRef(name="c"))
    j = Journal(run_dir)
    j.append(Boundary(run_id=run_dir.name, epoch=0, node_id="n0", phase="opened",
                      expr=parse_expr_dump(tree)))
    j.append(Dispatched(run_id=run_dir.name, epoch=0, node_id="n0", rig="c",
                        task="work", pre_head=pre))

    rig = _CountingRig("c")
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({"c": rig})).run_epoch(
        tree, epoch=0
    )
    assert rig.calls == 1  # the reverted effect was NOT reconciled — the rig re-ran
    assert "lost.txt" not in replay(run_dir).integrated.get((0, "n0"), [])


def parse_expr_dump(tree: object) -> dict[str, object]:
    from wildflows.expr import assign_node_ids, parse_expr

    admitted = parse_expr(tree.model_dump())  # type: ignore[attr-defined]
    assign_node_ids(admitted)
    return admitted.model_dump()


# ------------------------------------------------------------------- HIGH 6

def test_legacy_integrated_conflicting_or_empty_commits_is_rejected() -> None:
    """Compatibility parsing refuses no-proof / contradictory integration receipts."""
    # (a) empty receipt (no commit at all) — proves no effect.
    with pytest.raises(ValidationError):
        parse_event({"run_id": "r", "epoch": 0, "node_id": "n0", "kind": "integrated",
                     "commits": []})
    with pytest.raises(ValidationError):
        parse_event({"run_id": "r", "epoch": 0, "node_id": "n0", "kind": "integrated"})
    # (b) contradictory: `commit` disagrees with the last of `commits`.
    with pytest.raises(ValidationError):
        parse_event({"run_id": "r", "epoch": 0, "node_id": "n0", "kind": "integrated",
                     "commits": [{"sha": "aaa", "paths": ["x"]}], "commit": "bbb"})
    # A genuine legacy single-commit line still migrates cleanly (sanity).
    ev = parse_event({"run_id": "r", "epoch": 0, "node_id": "n0", "kind": "integrated",
                      "commit": "deadbeef", "paths": ["x.txt"]})
    assert ev.paths == ["x.txt"]  # type: ignore[union-attr]


def test_journal_load_rejects_noncontiguous_or_mismatched_sequences(tmp_path: Path) -> None:
    """`load` refuses a reordered/duplicated seq stream — the projection floors trust seq."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    lines = [
        {"seq": 0, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "opened"},
        {"seq": 3, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "opened"},
        {"seq": 2, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "closed"},  # seq 2 after seq 3 — reordered
    ]
    (run_dir / "events.ndjson").write_text(
        "\n".join(json.dumps(r) for r in lines) + "\n", encoding="utf-8"
    )
    with pytest.raises(JournalCompatibilityError, match="reordered or duplicated"):
        Journal.load(run_dir)


# ------------------------------------------------------------------- HIGH 7

def test_admission_rejects_self_forward_and_dispatch_sibling_ctx_refs(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    reg = RigRegistry({"c": EchoRig()})

    def eng() -> Engine:
        return Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg)

    # (a) self-ref: a lone do (n0) referencing itself.
    with pytest.raises(AdmissionError, match="not upstream"):
        eng().run_epoch(
            Do(task="x", rig=RigRef(name="c"), ctx=[CtxRef(kind="node", ref="n0")]), epoch=0)
    # (b) forward ref: n0.0 references its LATER Seq sibling n0.1.
    with pytest.raises(AdmissionError, match="not upstream"):
        eng().run_epoch(Seq(children=[
            Do(task="a", rig=RigRef(name="c"), ctx=[CtxRef(kind="node", ref="n0.1")]),
            Do(task="b", rig=RigRef(name="c")),
        ]), epoch=1)
    # (c) dispatch sibling: n0.1 references its concurrent Dispatch sibling n0.0.
    with pytest.raises(AdmissionError, match="crosses a Dispatch"):
        eng().run_epoch(Dispatch(children=[
            Do(task="a", rig=RigRef(name="c")),
            Do(task="b", rig=RigRef(name="c"), ctx=[CtxRef(kind="node", ref="n0.0")]),
        ]), epoch=2)
    # (d) POSITIVE control: an elder Seq sibling ref IS upstream and admitted.
    eng().run_epoch(Seq(children=[
        Do(task="produce", rig=RigRef(name="c")),
        Do(task="consume", rig=RigRef(name="c"), ctx=[CtxRef(kind="node", ref="n0.0")]),
    ]), epoch=3)
    assert replay(tmp_path / "run").epoch_closed(3)


def test_inplace_runtime_path_rejection_is_a_durable_failed_result(tmp_path: Path) -> None:
    """An inplace edit whose path only ESCAPES via a symlink (uncatchable at admission)
    is a durable failed result, never an exception escaping after `dispatched`."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    outside = tmp_path / "outside"
    outside.mkdir()
    os.symlink(outside, workdir / "link")  # in-worktree symlink to an external dir

    run_dir = tmp_path / "run"
    eng = Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({}))
    eng.run_epoch(Inplace(edits=[Edit(path="link/pwn.txt", content="x")]), epoch=0)  # no raise

    assert not (outside / "pwn.txt").exists()  # nothing written outside the workdir
    state = replay(run_dir)
    assert state.results[(0, "n0")].ok is False
    assert "rejected" in state.results[(0, "n0")].text
    assert state.epoch_closed(0)  # the epoch closed cleanly despite the rejection
