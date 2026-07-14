"""Execution scenarios for disposable node worktrees and loop/sequence shapes."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from wildflows.admission import AdmissionError
from wildflows.engine import Engine, NodeExecutionError, PredicateEvaluationError, replay
from wildflows.expr import Ask, CtxRef, Do, Edit, Inplace, Loop, RigRef, Seq, Until
from wildflows.result import Result
from wildflows.rig import EchoRig, Rig, RigRegistry, ShellRig


def git(repo: Path, *args: str, check: bool = True) -> str:
    process = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=check
    )
    return process.stdout.strip()


def init_repo(path: Path) -> str:
    path.mkdir()
    git(path, "init", "-q", "-b", "run")
    git(path, "config", "user.email", "test@wildflows.invalid")
    git(path, "config", "user.name", "Wildflows Test")
    (path / "seed").write_text("seed", encoding="utf-8")
    git(path, "add", "seed")
    git(path, "commit", "-qm", "seed")
    return git(path, "rev-parse", "HEAD")


def registry(**rigs: Rig) -> RigRegistry:
    return RigRegistry({"echo": EchoRig(), **rigs})


def engine(tmp_path: Path, rigs: RigRegistry | None = None) -> Engine:
    repo = tmp_path / "repo"
    init_repo(repo)
    return Engine(tmp_path / "run-state", repo, rigs or registry())


def test_inplace_and_do_integrate_on_run_branch(tmp_path: Path) -> None:
    eng = engine(tmp_path)
    tree = Seq(
        children=[
            Inplace(edits=[Edit(path="hello.txt", content="hello")]),
            Do(
                task="read it",
                rig=RigRef(name="echo"),
                ctx=[CtxRef(kind="file", ref="hello.txt")],
            ),
        ]
    )
    eng.run_epoch(tree, 0)

    assert (eng.workdir / "hello.txt").read_text(encoding="utf-8") == "hello"
    assert replay(eng.run_dir).epoch_closed(0)
    events = eng.journal.events()
    integrated = [event for event in events if event.kind == "integrated"]
    assert len(integrated) == 1
    assert integrated[0].paths == ["hello.txt"]
    assert list((eng.run_dir / "artifacts").rglob("*.json"))
    assert not any((eng.run_dir / "worktrees").iterdir())


def test_do_commits_rig_changes_and_preserves_rig_artifacts(tmp_path: Path) -> None:
    writer = ShellRig("printf changed > agent.txt", 5)
    eng = engine(tmp_path, registry(writer=writer))
    eng.run_epoch(Do(task="write", rig=RigRef(name="writer")), 0)

    assert git(eng.workdir, "show", f"{eng.run_branch}:agent.txt") == "changed"
    receipt = replay(eng.run_dir).receipts[(0, "n0")]
    assert receipt.paths == ["agent.txt"]


def test_failed_rig_leaves_branch_untouched_and_epoch_open(tmp_path: Path) -> None:
    failing = ShellRig("printf leak > leaked; exit 7", 5)
    eng = engine(tmp_path, registry(fail=failing))
    before = git(eng.workdir, "rev-parse", "HEAD")

    with pytest.raises(NodeExecutionError):
        eng.run_epoch(Do(task="fail", rig=RigRef(name="fail")), 0)

    assert git(eng.workdir, "rev-parse", "HEAD") == before
    assert not (eng.workdir / "leaked").exists()
    state = replay(eng.run_dir)
    assert not state.epoch_closed(0)
    assert state.results[(0, "n0")].outcome == "failed"


def test_resume_retries_failure_in_a_new_worktree(tmp_path: Path) -> None:
    class FlakyRig:
        def __init__(self) -> None:
            self.paths: list[Path] = []

        def run(self, prompt: str, workdir: Path) -> Result:
            self.paths.append(workdir)
            if len(self.paths) == 1:
                (workdir / "bad").write_text("discard", encoding="utf-8")
                return Result(text="try again", outcome="failed")
            (workdir / "good").write_text("land", encoding="utf-8")
            return Result(text="done")

    flaky = FlakyRig()
    eng = engine(tmp_path, registry(flaky=flaky))
    tree = Do(task="eventually", rig=RigRef(name="flaky"))
    with pytest.raises(NodeExecutionError):
        eng.run_epoch(tree, 0)
    Engine(eng.run_dir, eng.workdir, registry(flaky=flaky)).run_epoch(tree, 0)

    assert len(flaky.paths) == 2
    assert flaky.paths[0] != flaky.paths[1]
    assert not (eng.workdir / "bad").exists()
    assert (eng.workdir / "good").read_text(encoding="utf-8") == "land"


def test_inplace_symlink_escape_fails_without_undo(tmp_path: Path) -> None:
    eng = engine(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("owner", encoding="utf-8")
    (eng.workdir / "escape").symlink_to(outside)
    git(eng.workdir, "add", "escape")
    git(eng.workdir, "commit", "-qm", "add symlink")
    # The operator commit predates the epoch, so use a fresh run-state at its new tip.
    eng = Engine(tmp_path / "run-two", eng.workdir, registry())

    with pytest.raises(NodeExecutionError, match="symlink"):
        eng.run_epoch(Inplace(edits=[Edit(path="escape", content="bad")]), 0)
    assert outside.read_text(encoding="utf-8") == "owner"


def test_predicate_mutation_is_discarded(tmp_path: Path) -> None:
    eng = engine(tmp_path)
    loop = Loop(
        body=Inplace(edits=[Edit(path="body", content="ok")]),
        until=Until(kind="cmd", cmd="printf junk > predicate-junk; exit 0"),
        cap=2,
    )
    eng.run_epoch(loop, 0)

    assert (eng.workdir / "body").read_text(encoding="utf-8") == "ok"
    assert not (eng.workdir / "predicate-junk").exists()
    assert replay(eng.run_dir).loop_iterations[(0, "n0")] == 1


def test_predicate_timeout_leaves_epoch_open(tmp_path: Path) -> None:
    eng = engine(tmp_path)
    loop = Loop(
        body=Inplace(edits=[]),
        until=Until(kind="cmd", cmd="sleep 5", timeout_s=0.05),
        cap=1,
    )
    with pytest.raises(PredicateEvaluationError, match="timed out"):
        eng.run_epoch(loop, 0)
    assert not replay(eng.run_dir).epoch_closed(0)


def test_loop_cap_and_nested_floor_resume(tmp_path: Path) -> None:
    counter = ShellRig(
        "n=$(cat count 2>/dev/null || echo 0); n=$((n+1)); echo $n > count", 5
    )
    eng = engine(tmp_path, registry(counter=counter))
    inner = Loop(
        body=Do(task="tick", rig=RigRef(name="counter")),
        until=Until(kind="cmd", cmd='test "$(cat count)" -ge 2'),
        cap=3,
    )
    outer = Loop(
        body=inner,
        until=Until(kind="cmd", cmd="false"),
        cap=2,
    )
    eng.run_epoch(outer, 0)

    assert (eng.workdir / "count").read_text(encoding="utf-8").strip() == "3"
    outer_results = [
        event
        for event in eng.journal.events()
        if event.kind == "result" and event.node_id == "n0"
    ]
    assert outer_results[-1].outcome == "failed"
    assert "hit cap 2" in (outer_results[-1].loop_status or "")


def test_admission_rejects_before_boundary(tmp_path: Path) -> None:
    eng = engine(tmp_path)
    with pytest.raises(AdmissionError):
        eng.run_epoch(Ask(question="not executable"), 0)
    assert not (eng.run_dir / "events.ndjson").exists()
