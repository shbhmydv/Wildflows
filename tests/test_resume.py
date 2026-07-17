from __future__ import annotations

import json
import time
import urllib.request
from pathlib import Path
from typing import NoReturn, cast

import pytest

from wildflows.engine import Engine
from wildflows.events import (
    DispatchCalled,
    DispatchReturned,
    FrameExited,
    FrameIntegrated,
    FrameIntegrating,
    FramePushed,
    RunFinished,
    RunInterrupted,
)
from wildflows.frame import DispatchRequest, FrameResult, FrameRuntime, call_hash
from wildflows.rig import RigRegistry


class SimulatedKill(BaseException):
    pass


class IntegrationTear(BaseException):
    pass


class CountRig:
    timeout_s = 30.0

    def __init__(self, counter: Path) -> None:
        self.counter = counter

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, runtime
        count = int(self.counter.read_text(encoding="utf-8")) if self.counter.exists() else 0
        self.counter.write_text(str(count + 1), encoding="utf-8")
        (workdir / "root-effect.txt").write_text("durable\n", encoding="utf-8")
        return FrameResult(text="root done", exit_code=0)


class ReplayRig:
    timeout_s = 30.0

    def __init__(self, counter: Path, *, kill_root: bool) -> None:
        self.counter = counter
        self.kill_root = kill_root

    @staticmethod
    def _dispatch(runtime: FrameRuntime) -> dict[str, object]:
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "tools/call",
            "params": {
                "name": "dispatch",
                "arguments": {"tasks": ["durable child"], "rig": "replay"},
                "_meta": {"wildflows": {"callIndex": 0}},
            },
        }
        request = urllib.request.Request(
            runtime.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {runtime.token}",
                "X-Wildflows-Frame": runtime.frame_id,
            },
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            decoded = json.load(response)
        assert isinstance(decoded, dict)
        return cast(dict[str, object], decoded)

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt
        if runtime.frame_id != Engine.ROOT_FRAME_ID:
            count = int(self.counter.read_text(encoding="utf-8")) if self.counter.exists() else 0
            self.counter.write_text(str(count + 1), encoding="utf-8")
            (workdir / "child.txt").write_text("paid once\n", encoding="utf-8")
            return FrameResult(text="child complete", exit_code=0)
        response = self._dispatch(runtime)
        assert "result" in response
        assert (workdir / "child.txt").read_text(encoding="utf-8") == "paid once\n"
        if self.kill_root:
            raise SimulatedKill()
        (workdir / "root.txt").write_text("resumed\n", encoding="utf-8")
        return FrameResult(text="root resumed", exit_code=0)


@pytest.mark.parametrize("terminal_event", [False, True], ids=["legacy-stop", "interrupted"])
def test_resume_accepts_stopped_and_run_interrupted_journals(
    repo: Path, tmp_path: Path, terminal_event: bool
) -> None:
    run_id = "resume-interrupted" if terminal_event else "resume-legacy-stop"
    run_dir = tmp_path / run_id
    counter = tmp_path / f"{run_id}-executions"
    registry = RigRegistry({"count": CountRig(counter)})
    first = Engine(
        run_dir,
        repo,
        registry,
        run_id=run_id,
        root_rig="count",
        root_prompt="finish after lifecycle interruption",
        worktrees_root=tmp_path / f"{run_id}-worktrees",
    )
    base = first.repository.branch_tip()
    branch = first.repository.frame_branch("f0")
    first.repository.git(["branch", branch.removeprefix("refs/heads/"), base])
    first.journal.append(FramePushed(
        run_id=run_id,
        frame_id="f0",
        attempt=0,
        depth=0,
        rig="count",
        prompt="finish after lifecycle interruption",
        branch=branch,
        base_commit=base,
        worktree=str(tmp_path / "lost-worktree"),
    ))
    if terminal_event:
        first.journal.append(RunInterrupted(
            run_id=run_id,
            reason="signal:SIGINT",
        ))

    resumed = Engine(
        run_dir,
        repo,
        registry,
        run_id=run_id,
        root_rig="count",
        root_prompt="finish after lifecycle interruption",
    )
    assert resumed.run().outcome == "ok"

    assert counter.read_text(encoding="utf-8") == "1"
    events = resumed.journal.events()
    assert isinstance(events[-1], RunFinished)
    assert len([
        event for event in events if isinstance(event, RunInterrupted)
    ]) == int(terminal_event)
    assert resumed.projection.interrupted is None


class PartialParallelReplayRig:
    """Replays a pending parallel call whose first child already exited."""

    timeout_s = 30.0

    def __init__(self) -> None:
        self.dispatches = 0
        self.reused_child_runs = 0
        self.unfinished_child_runs = 0

    @staticmethod
    def _dispatch(runtime: FrameRuntime) -> dict[str, object]:
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "tools/call",
            "params": {
                "name": "dispatch",
                "arguments": {
                    "tasks": ["completed child", "unfinished child"],
                    "rig": "partial-replay",
                    "parallel": True,
                    "skills": [["long"], ["skill-selection"]],
                },
                "_meta": {"wildflows": {"callIndex": 0}},
            },
        }
        request = urllib.request.Request(
            runtime.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {runtime.token}",
                "X-Wildflows-Frame": runtime.frame_id,
            },
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            decoded = json.load(response)
        assert isinstance(decoded, dict)
        return cast(dict[str, object], decoded)

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt
        if runtime.frame_id == Engine.ROOT_FRAME_ID:
            self.dispatches += 1
            response = self._dispatch(runtime)
            assert "result" in response
            assert (workdir / "completed-child.txt").read_text(encoding="utf-8") == "reused\n"
            assert (workdir / "unfinished-child.txt").read_text(encoding="utf-8") == "ran once\n"
            (workdir / "root-after-replay.txt").write_text("done\n", encoding="utf-8")
            return FrameResult(text="root resumed", exit_code=0)
        if runtime.frame_id == "f0.c0.t0":
            self.reused_child_runs += 1
            raise AssertionError("the durable successful child must not rerun")
        assert runtime.frame_id == "f0.c0.t1"
        self.unfinished_child_runs += 1
        (workdir / "unfinished-child.txt").write_text("ran once\n", encoding="utf-8")
        return FrameResult(text="unfinished child complete", exit_code=0)


def test_resume_reconciles_ref_move_after_frame_integrating_tear(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "tear-run"
    counter = tmp_path / "root-executions"
    first = Engine(
        run_dir,
        repo,
        RigRegistry({"count": CountRig(counter)}),
        run_id="integration-tear",
        root_rig="count",
        root_prompt="root job",
        worktrees_root=tmp_path / "tear-worktrees",
    )
    advance = first.repository.advance

    def tear_after_move(
        target_ref: str,
        base: str,
        candidate: str,
        *,
        target_worktree: Path | None,
    ) -> None:
        advance(
            target_ref,
            base,
            candidate,
            target_worktree=target_worktree,
        )
        raise IntegrationTear()

    monkeypatch.setattr(first.repository, "advance", tear_after_move)
    with pytest.raises(IntegrationTear):
        first.run()
    assert counter.read_text(encoding="utf-8") == "1"
    assert any(isinstance(event, FrameIntegrating) for event in first.journal.events())
    assert not any(isinstance(event, FrameIntegrated) for event in first.journal.events())

    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"count": CountRig(counter)}),
        run_id="integration-tear",
        root_rig="count",
        root_prompt="root job",
    )
    assert resumed.run().outcome == "ok"
    assert counter.read_text(encoding="utf-8") == "1"
    assert (repo / "root-effect.txt").read_text(encoding="utf-8") == "durable\n"
    assert len([
        event for event in resumed.journal.events()
        if isinstance(event, FrameIntegrated)
    ]) == 1


def test_resume_replays_stack_and_memoizes_completed_dispatch(
    repo: Path, tmp_path: Path
) -> None:
    run_dir = tmp_path / "run"
    counter = tmp_path / "child-executions"
    first = Engine(
        run_dir,
        repo,
        RigRegistry({"replay": ReplayRig(counter, kill_root=True)}),
        run_id="memo",
        root_rig="replay",
        root_prompt="root job",
        worktrees_root=tmp_path / "external-worktrees",
    )
    with pytest.raises(SimulatedKill):
        first.run()
    assert counter.read_text(encoding="utf-8") == "1"

    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"replay": ReplayRig(counter, kill_root=False)}),
        run_id="memo",
        root_rig="replay",
        root_prompt="root job",
    )
    result = resumed.run()
    assert result.outcome == "ok"
    assert counter.read_text(encoding="utf-8") == "1"
    assert (repo / "child.txt").read_text(encoding="utf-8") == "paid once\n"
    assert (repo / "root.txt").read_text(encoding="utf-8") == "resumed\n"

    events = resumed.journal.events()
    assert len([event for event in events if isinstance(event, DispatchCalled)]) == 1
    assert len([event for event in events if isinstance(event, DispatchReturned)]) == 1
    pushes = [event for event in events if isinstance(event, FramePushed)]
    assert len([event for event in pushes if event.frame_id == "f0"]) == 2
    assert len([event for event in pushes if event.frame_id != "f0"]) == 1


def test_resume_replays_only_unfinished_parallel_child_without_barrier(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "partial-parallel-run"
    worktrees = tmp_path / "partial-parallel-worktrees"
    rig = PartialParallelReplayRig()
    request = DispatchRequest(
        tasks=["completed child", "unfinished child"],
        rig="partial-replay",
        parallel=True,
        skills=[["long"], ["skill-selection"]],
    )
    first = Engine(
        run_dir,
        repo,
        RigRegistry({"partial-replay": rig}),
        run_id="partial-parallel",
        root_rig="partial-replay",
        root_prompt="resume a partial parallel dispatch",
        worktrees_root=worktrees,
    )
    base = first.repository.branch_tip()
    root_branch = first.repository.frame_branch(Engine.ROOT_FRAME_ID)
    first.repository.git(["branch", root_branch.removeprefix("refs/heads/"), base])
    first.journal.append(FramePushed(
        run_id=first.run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        attempt=0,
        depth=0,
        rig="partial-replay",
        prompt="resume a partial parallel dispatch",
        skills=[],
        branch=root_branch,
        base_commit=base,
        worktree=str(worktrees / "lost-root"),
    ))
    digest = call_hash("dispatch", request)
    child_rig = request.rig
    assert isinstance(child_rig, str)
    first.journal.append(DispatchCalled(
        run_id=first.run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        call_index=0,
        call_hash=digest,
        request=request,
        caller_head=base,
    ))

    completed_id = "f0.c0.t0"
    completed_branch = first.repository.frame_branch(completed_id)
    completed_worktree = first.repository.create_frame_worktree(
        completed_id, completed_branch, base, resume=False
    )
    (completed_worktree.path / "completed-child.txt").write_text(
        "reused\n", encoding="utf-8"
    )
    completed_head = first.repository.commit_all(
        completed_worktree.path, "durable completed child"
    )
    first.repository.remove_worktree(completed_worktree)
    first.journal.append(FramePushed(
        run_id=first.run_id,
        frame_id=completed_id,
        parent_frame_id=Engine.ROOT_FRAME_ID,
        parent_call_index=0,
        task_index=0,
        attempt=0,
        depth=1,
        rig=child_rig,
        prompt=request.tasks[0],
        skills=request.skill_bundle(0),
        branch=completed_branch,
        base_commit=base,
        worktree=str(completed_worktree.path),
    ))
    first.journal.append(FrameExited(
        run_id=first.run_id,
        frame_id=completed_id,
        attempt=0,
        outcome="ok",
        text="completed before interruption",
        exit_code=0,
        head=completed_head,
    ))

    unfinished_id = "f0.c0.t1"
    unfinished_branch = first.repository.frame_branch(unfinished_id)
    first.repository.git(["branch", unfinished_branch.removeprefix("refs/heads/"), base])
    first.journal.append(FramePushed(
        run_id=first.run_id,
        frame_id=unfinished_id,
        parent_frame_id=Engine.ROOT_FRAME_ID,
        parent_call_index=0,
        task_index=1,
        attempt=0,
        depth=1,
        rig=child_rig,
        prompt=request.tasks[1],
        skills=request.skill_bundle(1),
        branch=unfinished_branch,
        base_commit=base,
        worktree=str(worktrees / "lost-unfinished-child"),
    ))

    def unexpected_barrier(parties: int) -> NoReturn:
        pytest.fail(f"partial replay must not create a barrier for {parties} children")

    monkeypatch.setattr("wildflows.engine.threading.Barrier", unexpected_barrier)
    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"partial-replay": rig}),
        run_id="partial-parallel",
        root_rig="partial-replay",
        root_prompt="resume a partial parallel dispatch",
    )
    started = time.monotonic()
    assert resumed.run().outcome == "ok"
    assert time.monotonic() - started < 10

    assert rig.dispatches == 1
    assert rig.reused_child_runs == 0
    assert rig.unfinished_child_runs == 1
    assert (repo / "completed-child.txt").read_text(encoding="utf-8") == "reused\n"
    assert (repo / "unfinished-child.txt").read_text(encoding="utf-8") == "ran once\n"
    assert (repo / "root-after-replay.txt").read_text(encoding="utf-8") == "done\n"

    events = resumed.journal.events()
    assert len([event for event in events if isinstance(event, DispatchCalled)]) == 1
    returned = [event for event in events if isinstance(event, DispatchReturned)]
    assert len(returned) == 1
    assert [child.frame_id for child in returned[0].result.children] == [
        completed_id,
        unfinished_id,
    ]
    assert [child.outcome for child in returned[0].result.children] == ["ok", "ok"]
    pushes = [event for event in events if isinstance(event, FramePushed)]
    child_pushes = [event for event in pushes if event.parent_frame_id == "f0"]
    assert [(event.frame_id, event.prompt, event.skills) for event in child_pushes] == [
        (completed_id, request.tasks[0], request.skill_bundle(0)),
        (unfinished_id, request.tasks[1], request.skill_bundle(1)),
        (unfinished_id, request.tasks[1], request.skill_bundle(1)),
    ]
    assert len([
        event for event in events
        if isinstance(event, FrameExited) and event.frame_id == completed_id
    ]) == 1
    assert len([
        event for event in events
        if isinstance(event, FrameExited) and event.frame_id == unfinished_id
    ]) == 1
    assert len([
        event for event in events
        if isinstance(event, FrameIntegrated) and event.frame_id == completed_id
    ]) == 1
    assert len([
        event for event in events
        if isinstance(event, FrameIntegrated) and event.frame_id == unfinished_id
    ]) == 1
