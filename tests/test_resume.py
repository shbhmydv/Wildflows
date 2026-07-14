from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import cast

import pytest

from wildflows.engine import Engine
from wildflows.events import (
    DispatchCalled,
    DispatchReturned,
    FrameIntegrated,
    FrameIntegrating,
    FramePushed,
)
from wildflows.frame import FrameResult, FrameRuntime
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
