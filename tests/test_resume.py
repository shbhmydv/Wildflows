"""Crash-window, receipt-verification, and branch-ownership scenarios."""
from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.test_engine import git, init_repo, registry
from wildflows.engine import (
    BranchDivergedError,
    Engine,
    NodeExecutionError,
    RepositoryTransientError,
    ResumeVerificationError,
)
from wildflows.events import Boundary, Event, Integrated
from wildflows.expr import Do, Edit, Inplace, Loop, RigRef, Seq, Until
from wildflows.projection import ExecutionOutcome
from wildflows.journal import Journal
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
        event.kind == "boundary" and (event.reason or "").startswith("resume fallback")
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


def test_exact_verified_prefix_rewind_falls_back_and_reruns_tail(
    tmp_path: Path,
) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    base = git(eng.workdir, "rev-parse", "HEAD")
    eng.run_epoch(tree, 0)

    git(eng.workdir, "reset", "--hard", base)
    resumed = Engine(eng.run_dir, eng.workdir, registry(count=rig))
    resumed.run_epoch(tree, 0)

    assert rig.calls == 2
    assert git(eng.workdir, "show", "HEAD:effect") == "2"
    assert any(
        event.kind == "boundary"
        and (event.reason or "").startswith("resume fallback: exact verified prefix")
        for event in resumed.journal.events()
    )


def test_missing_current_claimed_commit_falls_back_to_last_verified_tip(
    tmp_path: Path,
) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    base = git(eng.workdir, "rev-parse", "HEAD")
    eng.run_epoch(tree, 0)
    missing = git(eng.workdir, "rev-parse", "HEAD")
    (eng.workdir / ".git" / "objects" / missing[:2] / missing[2:]).unlink()

    resumed = Engine(eng.run_dir, eng.workdir, registry(count=rig))
    resumed.run_epoch(tree, 0)

    assert rig.calls == 2
    assert git(eng.workdir, "merge-base", "--is-ancestor", base, "HEAD") == ""
    assert git(eng.workdir, "show", "HEAD:effect") == "2"
    assert any(
        event.kind == "boundary"
        and "missing current claimed commit" in (event.reason or "")
        for event in resumed.journal.events()
    )


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux parent-death signals")
def test_sigkill_during_missing_claim_restore_converges_without_orphan_writer(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    git(repo, "config", "filter.slow.clean", "cat")
    git(repo, "config", "filter.slow.smudge", "cat")
    git(repo, "config", "filter.slow.required", "true")
    (repo / ".gitattributes").write_text("target filter=slow\n", encoding="utf-8")
    (repo / "target").write_text("base", encoding="utf-8")
    git(repo, "add", ".gitattributes", "target")
    git(repo, "commit", "-qm", "filtered base")
    base = git(repo, "rev-parse", "HEAD")
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="target", content="candidate")])
    Engine(run_dir, repo, registry()).run_epoch(tree, 0)
    missing = git(repo, "rev-parse", "HEAD")

    started = tmp_path / "restore-filter-started"
    finished = tmp_path / "restore-filter-finished"
    gate = tmp_path / "finish-restore-filter"
    filter_script = tmp_path / "slow-restore-smudge.sh"
    filter_script.write_text(
        "#!/bin/sh\n"
        f"printf x >> {shlex.quote(str(started))}\n"
        f"while [ ! -e {shlex.quote(str(gate))} ]; do sleep 0.01; done\n"
        "cat\n"
        f"printf done > {shlex.quote(str(finished))}\n",
        encoding="utf-8",
    )
    filter_script.chmod(0o755)
    git(repo, "config", "filter.slow.smudge", shlex.quote(str(filter_script)))
    (repo / ".git" / "objects" / missing[:2] / missing[2:]).unlink()

    pid = os.fork()
    if pid == 0:
        try:
            Engine(run_dir, repo, registry())
        except BaseException:
            os._exit(91)
        os._exit(0)

    waited = False
    try:
        deadline = time.monotonic() + 5
        while not started.exists() and time.monotonic() < deadline:
            exited, _ = os.waitpid(pid, os.WNOHANG)
            if exited:
                waited = True
                pytest.fail("constructor exited before missing-claim restore blocked")
            time.sleep(0.01)
        assert started.exists(), "restore smudge filter did not start"
        os.kill(pid, signal.SIGKILL)
        _, status = os.waitpid(pid, 0)
        waited = True
        assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL
    finally:
        if not waited:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)

    git(repo, "config", "filter.slow.smudge", "cat")
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                resumed = Engine(run_dir, repo, registry())
                break
            except RepositoryTransientError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

        assert not finished.exists(), "replacement waited for an unbound restore writer"
        assert git(repo, "rev-parse", "HEAD") == base
        assert (repo / "target").read_text(encoding="utf-8") == "base"
        assert git(repo, "status", "--porcelain") == ""
        assert not (repo / ".git" / "index.lock").exists()
        assert any(
            event.kind == "boundary"
            and "missing current claimed commit" in (event.reason or "")
            for event in resumed.journal.events()
        )

        resumed.run_epoch(tree, 0)
        assert (repo / "target").read_text(encoding="utf-8") == "candidate"
        assert git(repo, "status", "--porcelain") == ""
    finally:
        gate.write_text("finish", encoding="utf-8")
        deadline = time.monotonic() + 5
        while not finished.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
    assert finished.exists(), "original filter did not exit after its Git parent died"


def test_exact_verified_prefix_rewind_reruns_only_two_node_suffix(
    tmp_path: Path,
) -> None:
    rig = CountingRig()
    repo = tmp_path / "repo"
    init_repo(repo)
    run_dir = tmp_path / "run"
    tree = Seq(children=[
        Do(task="first", rig=RigRef(name="count")),
        Do(task="second", rig=RigRef(name="count")),
    ])
    eng = Engine(run_dir, repo, registry(count=rig))
    eng.run_epoch(tree, 0)
    first_tip = next(
        event.commit
        for event in eng.journal.events()
        if event.kind == "integrated" and event.node_id == "n0.0"
    )

    git(repo, "reset", "--hard", first_tip)
    resumed = Engine(run_dir, repo, registry(count=rig))
    resumed.run_epoch(tree, 0)

    assert rig.calls == 3
    assert resumed.journal.projection.results[(0, "n0.0")].text == "call 1"
    assert resumed.journal.projection.results[(0, "n0.1")].text == "call 3"


def test_unknown_tip_after_verified_prefix_still_raises_typed_refusal(
    tmp_path: Path,
) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    base = git(eng.workdir, "rev-parse", "HEAD")
    eng.run_epoch(tree, 0)
    git(eng.workdir, "reset", "--hard", base)
    (eng.workdir / "operator").write_text("mine", encoding="utf-8")
    git(eng.workdir, "add", "operator")
    git(eng.workdir, "commit", "-qm", "unknown side tip")

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
        event.kind == "boundary" and event.fallback_from is not None
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


def test_operator_commit_between_siblings_is_refused_before_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = CountingRig()
    eng, _ = setup(tmp_path, rig)
    tree = Seq(children=[
        Do(task="first", rig=RigRef(name="count")),
        Do(task="second", rig=RigRef(name="count")),
    ])
    execute = eng._exec_do

    def move_after_first(node: Do, epoch: int) -> ExecutionOutcome:
        outcome = execute(node, epoch)
        if node.node_id == "n0.0":
            (eng.workdir / "operator").write_text("mine", encoding="utf-8")
            git(eng.workdir, "add", "operator")
            git(eng.workdir, "commit", "-qm", "operator between nodes")
        return outcome

    monkeypatch.setattr(eng, "_exec_do", move_after_first)
    with pytest.raises(BranchDivergedError, match="operator commits"):
        eng.run_epoch(tree, 0)
    dispatched = [event for event in eng.journal.events() if event.kind == "dispatched"]
    assert [event.node_id for event in dispatched] == ["n0.0"]


def test_invalid_non_tail_receipt_reruns_the_whole_unverified_tail(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    base = init_repo(repo)
    run_dir = tmp_path / "run"
    tree = Seq(children=[
        Inplace(edits=[Edit(path="a", content="a")]),
        Inplace(edits=[Edit(path="b", content="b")]),
    ])
    eng = Engine(run_dir, repo, registry())
    eng.run_epoch(tree, 0)
    path = run_dir / "events.ndjson"
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    first = next(record for record in records if record["kind"] == "integrated")
    first["commits"][-1]["sha"] = "f" * 40
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    git(repo, "reset", "--hard", base)

    resumed = Engine(run_dir, repo, registry())
    resumed.run_epoch(tree, 0)
    state = resumed.journal.projection
    assert (repo / "a").read_text(encoding="utf-8") == "a"
    assert (repo / "b").read_text(encoding="utf-8") == "b"
    assert "f" * 40 not in state.receipts[(0, "n0.0")].shas


def test_fallback_invalidates_later_effectless_results(tmp_path: Path) -> None:
    class EffectlessRig:
        def __init__(self) -> None:
            self.calls = 0
        def run(self, prompt: str, workdir: Path) -> Result:
            self.calls += 1
            return Result(text=f"observation {self.calls}")
    rig = EffectlessRig()
    repo = tmp_path / "repo"
    base = init_repo(repo)
    run_dir = tmp_path / "run"
    tree = Seq(children=[
        Inplace(edits=[Edit(path="a", content="a")]),
        Do(task="observe", rig=RigRef(name="observe")),
    ])
    eng = Engine(run_dir, repo, registry(observe=rig))
    eng.run_epoch(tree, 0)
    path = run_dir / "events.ndjson"
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    receipt = next(record for record in records if record["kind"] == "integrated")
    receipt["commits"][-1]["sha"] = "f" * 40
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    git(repo, "reset", "--hard", base)
    resumed = Engine(run_dir, repo, registry(observe=rig))
    resumed.run_epoch(tree, 0)
    assert rig.calls == 2
    assert resumed.journal.projection.results[(0, "n0.1")].text == "observation 2"


def test_invalid_loop_body_receipt_does_not_reuse_old_loop_result(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    base = init_repo(repo)
    run_dir = tmp_path / "run"
    tree = Loop(
        body=Inplace(edits=[Edit(path="looped", content="yes")]),
        until=Until(kind="cmd", cmd="true"), cap=1,
    )
    eng = Engine(run_dir, repo, registry())
    eng.run_epoch(tree, 0)
    path = run_dir / "events.ndjson"
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    receipt = next(record for record in records if record["kind"] == "integrated")
    receipt["commits"][-1]["sha"] = "f" * 40
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    git(repo, "reset", "--hard", base)

    resumed = Engine(run_dir, repo, registry())
    resumed.run_epoch(tree, 0)
    assert (repo / "looped").read_text(encoding="utf-8") == "yes"
    loop_results = [
        event for event in resumed.journal.events()
        if event.kind == "result" and event.node_id == "n0" and event.loop_status
    ]
    assert len(loop_results) == 2


def test_nested_loop_partial_iteration_uses_resume_floor(tmp_path: Path) -> None:
    class CrashThird:
        def __init__(self) -> None:
            self.calls = 0

        def run(self, prompt: str, workdir: Path) -> Result:
            self.calls += 1
            if self.calls == 3:
                raise SystemExit("partial outer iteration")
            path = workdir / "count"
            number = int(path.read_text(encoding="utf-8")) if path.exists() else 0
            path.write_text(str(number + 1), encoding="utf-8")
            return Result(text=str(number + 1))

    rig = CrashThird()
    repo = tmp_path / "repo"
    init_repo(repo)
    run_dir = tmp_path / "run"
    inner = Loop(
        body=Do(task="tick", rig=RigRef(name="counter")),
        until=Until(kind="cmd", cmd='test "$(cat count)" -ge 2'), cap=3,
    )
    tree = Loop(body=inner, until=Until(kind="cmd", cmd="false"), cap=2)
    eng = Engine(run_dir, repo, registry(counter=rig))
    with pytest.raises(SystemExit):
        eng.run_epoch(tree, 0)
    Engine(run_dir, repo, registry(counter=rig)).run_epoch(tree, 0)

    assert rig.calls == 4
    assert (repo / "count").read_text(encoding="utf-8") == "3"


def test_multi_commit_rig_receipt_records_every_commit(tmp_path: Path) -> None:
    class MultiCommitRig:
        def run(self, prompt: str, workdir: Path) -> Result:
            for name in ("one", "two"):
                (workdir / name).write_text(name, encoding="utf-8")
                git(workdir, "add", name)
                git(workdir, "commit", "-qm", name)
            return Result(text="two commits")

    repo = tmp_path / "repo"
    init_repo(repo)
    eng = Engine(tmp_path / "run", repo, registry(multi=MultiCommitRig()))
    eng.run_epoch(Do(task="multi", rig=RigRef(name="multi")), 0)
    receipt = eng.journal.projection.receipts[(0, "n0")]
    assert len(receipt.commits) == 2
    assert [commit.paths for commit in receipt.commits] == [["one"], ["two"]]


def test_operator_third_tip_in_result_integration_window_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)

    def crash(base_commit: str, candidate: str) -> None:
        raise SystemExit("before fast-forward")

    monkeypatch.setattr(eng.repo, "integrate", crash)
    with pytest.raises(SystemExit):
        eng.run_epoch(tree, 0)
    (eng.workdir / "operator").write_text("mine", encoding="utf-8")
    git(eng.workdir, "add", "operator")
    git(eng.workdir, "commit", "-qm", "operator in crash window")
    with pytest.raises(BranchDivergedError, match="incomplete attempt"):
        Engine(eng.run_dir, eng.workdir, registry(count=rig))


def test_fallback_boundary_is_complete_after_immediate_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    base = init_repo(repo)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="a", content="a")])
    Engine(run_dir, repo, registry()).run_epoch(tree, 0)
    path = run_dir / "events.ndjson"
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    receipt = next(record for record in records if record["kind"] == "integrated")
    receipt["commits"][-1]["sha"] = "f" * 40
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    git(repo, "reset", "--hard", base)
    append = Journal.append
    def crash_after_fallback(self: Journal, event: Event) -> int:
        seq = append(self, event)
        if isinstance(event, Boundary) and event.fallback_from is not None:
            raise SystemExit("after fallback append")
        return seq
    monkeypatch.setattr(Journal, "append", crash_after_fallback)
    with pytest.raises(SystemExit):
        Engine(run_dir, repo, registry())
    monkeypatch.setattr(Journal, "append", append)
    resumed = Engine(run_dir, repo, registry())
    resumed.run_epoch(tree, 0)
    assert (repo / "a").read_text(encoding="utf-8") == "a"


def test_invalid_receipt_truncates_later_epoch_claims(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    base = init_repo(repo)
    run_dir = tmp_path / "run"
    first = Inplace(edits=[Edit(path="a", content="a")])
    second = Inplace(edits=[Edit(path="b", content="b")])
    eng = Engine(run_dir, repo, registry())
    eng.run_epoch(first, 0)
    eng.run_epoch(second, 1)
    path = run_dir / "events.ndjson"
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    receipt = next(record for record in records if record["kind"] == "integrated")
    receipt["commits"][-1]["sha"] = "f" * 40
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    git(repo, "reset", "--hard", base)
    resumed = Engine(run_dir, repo, registry())
    resumed.run_epoch(first, 0)
    resumed.run_epoch(second, 1)
    assert (repo / "a").read_text(encoding="utf-8") == "a"
    assert (repo / "b").read_text(encoding="utf-8") == "b"


@pytest.mark.parametrize("change", ["post_head", "outcome"])
def test_integrated_claim_must_match_successful_result_certificate(
    tmp_path: Path, change: str
) -> None:
    rig = CountingRig()
    eng, tree = setup(tmp_path, rig)
    base = git(eng.workdir, "rev-parse", "HEAD")
    eng.run_epoch(tree, 0)
    path = eng.run_dir / "events.ndjson"
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    result = next(record for record in records if record["kind"] == "result")
    if change == "post_head":
        result["post_head"] = base
    else:
        result["outcome"] = "failed"
        result["receipt_required"] = False
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    with pytest.raises(ResumeVerificationError, match="contradicts"):
        Engine(eng.run_dir, eng.workdir, registry(count=rig))


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux parent-death signals")
def test_sigkill_during_slow_checked_out_fast_forward_cannot_land_after_fallback(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    started = tmp_path / "filter-started"
    finished = tmp_path / "filter-finished"
    gate = tmp_path / "finish-filter"
    filter_script = tmp_path / "slow-smudge.sh"
    filter_script.write_text(
        "#!/bin/sh\n"
        f"printf started > {shlex.quote(str(started))}\n"
        f"while [ ! -e {shlex.quote(str(gate))} ]; do sleep 0.01; done\n"
        "cat\n"
        f"printf done > {shlex.quote(str(finished))}\n",
        encoding="utf-8",
    )
    filter_script.chmod(0o755)
    git(repo, "config", "filter.slow.clean", "cat")
    git(repo, "config", "filter.slow.smudge", shlex.quote(str(filter_script)))
    git(repo, "config", "filter.slow.required", "true")
    (repo / ".gitattributes").write_text("target filter=slow\n", encoding="utf-8")
    (repo / "first").write_text("base", encoding="utf-8")
    git(repo, "add", ".gitattributes", "first")
    git(repo, "commit", "-qm", "configure slow filter")
    base = git(repo, "rev-parse", "HEAD")
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[
        Edit(path="first", content="candidate-first"),
        Edit(path="target", content="candidate"),
    ])

    pid = os.fork()
    if pid == 0:
        try:
            Engine(run_dir, repo, registry()).run_epoch(tree, 0)
        except BaseException:
            os._exit(91)
        os._exit(0)

    waited = False
    try:
        deadline = time.monotonic() + 5
        while not started.exists() and time.monotonic() < deadline:
            exited, _ = os.waitpid(pid, os.WNOHANG)
            if exited:
                waited = True
                pytest.fail("engine exited before the checked-out fast-forward blocked")
            time.sleep(0.01)
        assert started.exists(), "smudge filter did not start"
        os.kill(pid, signal.SIGKILL)
        _, status = os.waitpid(pid, 0)
        waited = True
        assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL
    finally:
        if not waited:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)

    assert git(repo, "rev-parse", "HEAD") == base
    assert (repo / "first").read_text(encoding="utf-8") == "candidate-first"
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                resumed = Engine(run_dir, repo, registry())
                break
            except RepositoryTransientError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        fallback = [
            event for event in resumed.journal.events()
            if event.kind == "boundary" and event.fallback_from is not None
        ]
        assert fallback
        candidate = next(
            event.post_head for event in resumed.journal.events()
            if event.kind == "result" and event.receipt_required
        )
        assert not finished.exists(), "replacement waited for an unbound ref mover"
        assert git(repo, "rev-parse", "HEAD") == base
        assert (repo / "first").read_text(encoding="utf-8") == "base"
        assert git(repo, "status", "--porcelain") == ""
        assert git(repo, "rev-parse", "HEAD") != candidate
        assert not any(event.kind == "integrated" for event in resumed.journal.events())
        assert not (repo / ".git" / "index.lock").exists()
    finally:
        gate.write_text("finish", encoding="utf-8")
        deadline = time.monotonic() + 5
        while not finished.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
    assert finished.exists(), "original filter did not exit after its Git parent died"
    assert git(repo, "rev-parse", "HEAD") == base

    resumed.run_epoch(tree, 0)

    assert (repo / "first").read_text(encoding="utf-8") == "candidate-first"
    assert (repo / "target").read_text(encoding="utf-8") == "candidate"
    assert git(repo, "status", "--porcelain") == ""
    assert any(event.kind == "integrated" for event in resumed.journal.events())
    assert not (repo / ".git" / "index.lock").exists()


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux parent-death signals")
def test_sigkill_during_unchecked_out_ref_update_recovers_ref_lock(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    git(repo, "branch", "workflow")
    base = git(repo, "rev-parse", "workflow")
    started = tmp_path / "ref-hook-started"
    finished = tmp_path / "ref-hook-finished"
    gate = tmp_path / "finish-ref-hook"
    hook = repo / ".git" / "hooks" / "reference-transaction"
    hook.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = prepared ] && grep -q ' refs/heads/workflow$'; then\n"
        f"  printf started > {shlex.quote(str(started))}\n"
        f"  while [ ! -e {shlex.quote(str(gate))} ]; do sleep 0.01; done\n"
        f"  printf done > {shlex.quote(str(finished))}\n"
        "fi\n",
        encoding="utf-8",
    )
    hook.chmod(0o755)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="target", content="candidate")])

    pid = os.fork()
    if pid == 0:
        try:
            Engine(run_dir, repo, registry(), "workflow").run_epoch(tree, 0)
        except BaseException:
            os._exit(91)
        os._exit(0)

    waited = False
    try:
        deadline = time.monotonic() + 5
        while not started.exists() and time.monotonic() < deadline:
            exited, _ = os.waitpid(pid, os.WNOHANG)
            if exited:
                waited = True
                pytest.fail("engine exited before the ref transaction hook blocked")
            time.sleep(0.01)
        assert started.exists(), "reference-transaction hook did not start"
        os.kill(pid, signal.SIGKILL)
        _, status = os.waitpid(pid, 0)
        waited = True
        assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL
    finally:
        if not waited:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)

    ref_lock = repo / ".git" / "refs" / "heads" / "workflow.lock"
    assert ref_lock.exists()
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                resumed = Engine(run_dir, repo, registry(), "workflow")
                break
            except RepositoryTransientError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        assert not ref_lock.exists()
        assert git(repo, "rev-parse", "workflow") == base
        assert any(
            event.kind == "boundary" and event.fallback_from is not None
            for event in resumed.journal.events()
        )
    finally:
        gate.write_text("finish", encoding="utf-8")
        deadline = time.monotonic() + 5
        while not finished.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
    assert finished.exists(), "old ref hook did not exit after its Git parent died"
    assert git(repo, "rev-parse", "workflow") == base

    resumed.run_epoch(tree, 0)
    assert git(repo, "show", "workflow:target") == "candidate"
    assert git(repo, "status", "--porcelain") == ""
    assert not ref_lock.exists()


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux procfs")
def test_interrupted_index_lock_recovery_refuses_a_live_git_owner(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    git(repo, "config", "filter.slow.clean", "cat")
    git(repo, "config", "filter.slow.smudge", "cat")
    git(repo, "config", "filter.slow.required", "true")
    (repo / ".gitattributes").write_text("target filter=slow\n", encoding="utf-8")
    (repo / "target").write_text("base", encoding="utf-8")
    git(repo, "add", ".gitattributes", "target")
    git(repo, "commit", "-qm", "filtered base")
    base = git(repo, "rev-parse", "HEAD")
    (repo / "target").write_text("operator", encoding="utf-8")
    git(repo, "add", "target")
    git(repo, "commit", "-qm", "operator candidate")
    candidate = git(repo, "rev-parse", "HEAD")
    git(repo, "reset", "--hard", base)

    started = tmp_path / "operator-filter-started"
    gate = tmp_path / "finish-operator-filter"
    filter_script = tmp_path / "slow-operator-smudge.sh"
    filter_script.write_text(
        "#!/bin/sh\n"
        f"printf started > {shlex.quote(str(started))}\n"
        f"while [ ! -e {shlex.quote(str(gate))} ]; do sleep 0.01; done\n"
        "cat\n",
        encoding="utf-8",
    )
    filter_script.chmod(0o755)
    git(repo, "config", "filter.slow.smudge", shlex.quote(str(filter_script)))
    engine = Engine(tmp_path / "run", repo, registry())
    writer = subprocess.Popen(
        ["git", "read-tree", "--reset", "-u", candidate], cwd=repo
    )
    try:
        deadline = time.monotonic() + 5
        while not started.exists() and time.monotonic() < deadline:
            assert writer.poll() is None, "operator Git exited before its filter blocked"
            time.sleep(0.01)
        assert started.exists(), "operator smudge filter did not start"

        with pytest.raises(RepositoryTransientError, match="live writer"):
            engine.repo.recover_interrupted_locks()
        assert writer.poll() is None
        assert (repo / ".git" / "index.lock").exists()
    finally:
        gate.write_text("finish", encoding="utf-8")
        writer.wait(timeout=5)

    assert not (repo / ".git" / "index.lock").exists()


def test_interrupted_index_lock_is_preserved_after_branch_becomes_unowned(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    git(repo, "branch", "workflow")
    engine = Engine(tmp_path / "run", repo, registry(), "workflow")
    lock = repo / ".git" / "index.lock"
    lock.touch()

    with pytest.raises(RepositoryTransientError, match="no longer owns"):
        engine.repo.recover_interrupted_locks()

    assert lock.exists()
    lock.unlink()


def test_interrupted_integration_preserves_third_state_worktree_edit(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = init_repo(repo)
    (repo / "seed").write_text("candidate", encoding="utf-8")
    git(repo, "add", "seed")
    git(repo, "commit", "-qm", "candidate")
    candidate = git(repo, "rev-parse", "HEAD")
    git(repo, "reset", "--hard", base)
    (repo / "seed").write_text("operator", encoding="utf-8")
    engine = Engine(tmp_path / "run", repo, registry())

    with pytest.raises(RepositoryTransientError, match="changed outside"):
        engine.repo.restore_interrupted_integration(base, candidate)

    assert (repo / "seed").read_text(encoding="utf-8") == "operator"


def test_named_run_branch_can_advance_without_checkout(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    git(repo, "branch", "workflow")
    eng = Engine(tmp_path / "run", repo, registry(writer=CountingRig()), "workflow")
    eng.run_epoch(Do(task="write", rig=RigRef(name="writer")), 0)

    assert git(repo, "show", "workflow:effect") == "1"
    assert git(repo, "branch", "--show-current") == "run"
    assert not (repo / "effect").exists()


def test_named_run_branch_checked_out_in_other_worktree_is_updated_or_refused_cleanly(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    git(repo, "branch", "workflow")
    owner = tmp_path / "workflow-owner"
    git(repo, "worktree", "add", str(owner), "workflow")
    before = git(repo, "rev-parse", "workflow")
    eng = Engine(tmp_path / "run", repo, registry(), "workflow")

    with pytest.raises(NodeExecutionError, match="checked out in linked worktree"):
        eng.run_epoch(Inplace(edits=[Edit(path="owned", content="new")]), 0)

    assert git(repo, "rev-parse", "workflow") == before
    assert git(owner, "status", "--porcelain") == ""
    assert not (owner / "owned").exists()
