"""Crash-window, receipt-verification, and branch-ownership scenarios."""
from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import pytest

from tests.test_engine import git, init_repo, registry
from wildflows.engine import (
    BranchDivergedError,
    Engine,
    ResumeVerificationError,
)
from wildflows.events import Event, Integrated
from wildflows.expr import Do, RigRef
from wildflows.result import Result


class CountingRig:
    def __init__(self) -> None:
        self.calls = 0
        self.paths: list[Path] = []

    def run(self, prompt: str, workdir: Path) -> Result:
        self.calls += 1
        self.paths.append(workdir)
        (workdir / "effect").write_text(str(self.calls), encoding="utf-8")
        return Result(text=f"call {self.calls}")


def setup(tmp_path: Path, rig: CountingRig) -> tuple[Engine, Do]:
    repo = tmp_path / "repo"
    init_repo(repo)
    eng = Engine(tmp_path / "run", repo, registry(count=rig))
    return eng, Do(task="count", rig=RigRef(name="count"))


def test_crash_after_branch_fast_forward_reconstructs_integrated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    append = eng.journal.append

    def crash_on_integrated(event: Event) -> int:
        if isinstance(event, Integrated):
            raise OSError("simulated fsync crash")
        return append(event)

    monkeypatch.setattr(eng.journal, "append", crash_on_integrated)
    with pytest.raises(OSError, match="simulated"):
        eng.run_epoch(tree, 0)
    assert git(eng.workdir, "show", "HEAD:effect") == "1"

    resumed = Engine(eng.run_dir, eng.workdir, registry(count=rig))
    resumed.run_epoch(tree, 0)
    assert rig.calls == 1
    assert any(event.kind == "integrated" for event in resumed.journal.events())


def test_crash_before_fast_forward_journals_fallback_and_reruns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    base = git(eng.workdir, "rev-parse", "HEAD")

    def crash(base_commit: str, candidate: str) -> None:
        raise SystemExit("between result and integration")

    monkeypatch.setattr(eng.repo, "integrate", crash)
    with pytest.raises(SystemExit):
        eng.run_epoch(tree, 0)
    assert git(eng.workdir, "rev-parse", "HEAD") == base

    resumed = Engine(eng.run_dir, eng.workdir, registry(count=rig))
    resumed.run_epoch(tree, 0)
    assert rig.calls == 2
    assert any(
        event.kind == "result" and event.text.startswith("resume fallback")
        for event in resumed.journal.events()
    )


def test_dispatched_only_attempt_is_abandoned_and_rerun(tmp_path: Path) -> None:
    class CrashOnce(CountingRig):
        def run(self, prompt: str, workdir: Path) -> Result:
            self.calls += 1
            self.paths.append(workdir)
            if self.calls == 1:
                raise SystemExit("dead model")
            (workdir / "effect").write_text("good", encoding="utf-8")
            return Result(text="good")

    rig = CrashOnce()
    eng, tree = setup(tmp_path, rig)
    with pytest.raises(SystemExit):
        eng.run_epoch(tree, 0)
    resumed = Engine(eng.run_dir, eng.workdir, registry(count=rig))
    resumed.run_epoch(tree, 0)

    assert rig.calls == 2
    assert rig.paths[0] != rig.paths[1]
    assert git(eng.workdir, "show", "HEAD:effect") == "good"


def test_orphan_write_targets_only_never_reused_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class OrphanOnce(CountingRig):
        def run(self, prompt: str, workdir: Path) -> Result:
            self.calls += 1
            self.paths.append(workdir)
            if self.calls == 1:
                subprocess.Popen(
                    ["sh", "-c", "sleep 0.2; printf orphan > late"],
                    cwd=workdir,
                    start_new_session=True,
                )
                raise SystemExit("controller died")
            (workdir / "effect").write_text("accepted", encoding="utf-8")
            return Result(text="accepted")

    rig = OrphanOnce()
    eng, tree = setup(tmp_path, rig)
    monkeypatch.setattr(eng.repo, "remove_worktree", lambda worktree: None)
    with pytest.raises(SystemExit):
        eng.run_epoch(tree, 0)

    resumed = Engine(eng.run_dir, eng.workdir, registry(count=rig))
    resumed.run_epoch(tree, 0)
    time.sleep(0.35)

    assert (rig.paths[0] / "late").read_text(encoding="utf-8") == "orphan"
    assert rig.paths[0] != rig.paths[1]
    assert git(eng.workdir, "show", "HEAD:effect") == "accepted"
    assert git(eng.workdir, "show", "HEAD:late", check=False) == ""
    git(eng.workdir, "worktree", "remove", "--force", str(rig.paths[0]))


def test_operator_commit_on_run_branch_is_refused(tmp_path: Path) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    eng.run_epoch(tree, 0)
    (eng.workdir / "operator").write_text("mine", encoding="utf-8")
    git(eng.workdir, "add", "operator")
    git(eng.workdir, "commit", "-qm", "operator activity")

    with pytest.raises(BranchDivergedError, match="operator commits"):
        Engine(eng.run_dir, eng.workdir, registry(count=rig))


def test_unverifiable_receipt_at_previous_tip_falls_back(tmp_path: Path) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    base = git(eng.workdir, "rev-parse", "HEAD")
    eng.run_epoch(tree, 0)

    journal = eng.run_dir / "events.ndjson"
    records = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]
    for record in records:
        if record["kind"] == "integrated":
            record["commits"][-1]["sha"] = "f" * 40
            record["commit"] = "f" * 40
    journal.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    git(eng.workdir, "reset", "--hard", base)

    resumed = Engine(eng.run_dir, eng.workdir, registry(count=rig))
    resumed.run_epoch(tree, 0)
    assert rig.calls == 2
    assert any(
        event.kind == "result" and event.fallback_for is not None
        for event in resumed.journal.events()
    )


def test_unverifiable_live_receipt_refuses_instead_of_guessing(tmp_path: Path) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    eng.run_epoch(tree, 0)
    journal = eng.run_dir / "events.ndjson"
    records = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]
    for record in records:
        if record["kind"] == "integrated":
            record["commits"][-1]["paths"] = ["lie"]
    journal.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )

    with pytest.raises(ResumeVerificationError, match="effect is live"):
        Engine(eng.run_dir, eng.workdir, registry(count=rig))


def test_named_run_branch_can_advance_without_checkout(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    git(repo, "branch", "workflow")
    eng = Engine(tmp_path / "run", repo, registry(writer=CountingRig()), "workflow")
    eng.run_epoch(Do(task="write", rig=RigRef(name="writer")), 0)

    assert git(repo, "show", "workflow:effect") == "1"
    assert git(repo, "branch", "--show-current") == "run"
    assert not (repo / "effect").exists()
