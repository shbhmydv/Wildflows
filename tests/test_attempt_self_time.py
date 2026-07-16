from __future__ import annotations

import json
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from wildflows.engine import Engine
from wildflows.events import (
    FrameExited,
    FramePushed,
    FrameSlotAcquired,
    FrameSlotReleased,
)
from wildflows.frame import FrameResult, FrameRuntime
from wildflows.journal import Journal
from wildflows.rig import RigRegistry


def _dispatch(
    runtime: FrameRuntime, index: int, arguments: dict[str, object]
) -> dict[str, object]:
    payload = {
        "jsonrpc": "2.0",
        "id": index,
        "method": "tools/call",
        "params": {
            "name": "dispatch",
            "arguments": arguments,
            "_meta": {"wildflows": {"callIndex": index}},
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
    result = cast(dict[str, object], decoded)["result"]
    assert isinstance(result, dict)
    structured = cast(dict[str, object], result)["structuredContent"]
    assert isinstance(structured, dict)
    return cast(dict[str, object], structured)


@dataclass
class _TimeoutThenRetryRig:
    timeout_s: float = 0.25
    child_attempts: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt
        if runtime.frame_id == "f0":
            first = _dispatch(runtime, 0, {
                "tasks": ["time out once, then finish"],
                "rig": "budget",
            })
            assert first["outcome"] == "failed"
            children = first["children"]
            assert isinstance(children, list) and len(children) == 1
            child = children[0]
            assert isinstance(child, dict)
            retried = _dispatch(runtime, 1, {"retry_frame": child["frame_id"]})
            assert retried["outcome"] == "ok"
            assert (workdir / "retry-complete.txt").read_text(encoding="utf-8") == "ok\n"
            return FrameResult(text="root observed successful retry", exit_code=0)
        with self.lock:
            self.child_attempts += 1
            attempt = self.child_attempts
        if attempt == 1:
            time.sleep(0.35)
            return FrameResult(text="first attempt returned too late", exit_code=0)
        time.sleep(0.10)
        (workdir / "retry-complete.txt").write_text("ok\n", encoding="utf-8")
        return FrameResult(text="retry used a fresh clock", exit_code=0)


@dataclass
class _RecoveredCrashRig:
    timeout_s: float = 0.15
    runs: int = 0

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, runtime
        self.runs += 1
        time.sleep(0.06)
        (workdir / "crash-recovered.txt").write_text("done\n", encoding="utf-8")
        return FrameResult(text="crash relaunch completed", exit_code=0)


def test_retry_after_self_timeout_gets_full_budget_and_replays_per_attempt(
    repo: Path, tmp_path: Path
) -> None:
    run_dir = tmp_path / "run-timeout-retry"
    rig = _TimeoutThenRetryRig()
    engine = Engine(
        run_dir,
        repo,
        RigRegistry({"budget": rig}),
        run_id="timeout-retry",
        root_rig="budget",
        root_prompt="retry a timed-out child",
        worktrees_root=tmp_path / "worktrees-timeout-retry",
    )

    assert engine.run().outcome == "ok"

    child = engine.projection.frame("f0.c0.t0")
    exits = [
        event for event in engine.journal.events()
        if isinstance(event, FrameExited) and event.frame_id == child.frame_id
    ]
    assert [event.attempt for event in exits] == [0, 1]
    assert exits[0].outcome == "failed"
    assert "self-time budget" in exits[0].text
    assert exits[1].outcome == "ok"
    assert child.attempt_self_time_s[0] >= rig.timeout_s * 0.9
    assert 0 < child.attempt_self_time_s[1] < rig.timeout_s
    assert child.self_time_s == sum(child.attempt_self_time_s.values())

    replayed = Journal.load(run_dir).projection.frame(child.frame_id)
    assert replayed.attempt_self_time_s == child.attempt_self_time_s
    assert replayed.self_time_s == child.self_time_s


def test_crash_relaunch_starts_fresh_clock_after_prior_attempt_exhaustion(
    repo: Path, tmp_path: Path
) -> None:
    run_dir = tmp_path / "run-crash-budget"
    worktrees = tmp_path / "worktrees-crash-budget"
    rig = _RecoveredCrashRig()
    registry = RigRegistry({"budget": rig})
    first = Engine(
        run_dir,
        repo,
        registry,
        run_id="crash-budget",
        root_rig="budget",
        root_prompt="recover with a fresh attempt budget",
        worktrees_root=worktrees,
    )
    base = first.repository.branch_tip()
    branch = first.repository.frame_branch("f0")
    first.repository.git(["branch", branch.removeprefix("refs/heads/"), base])
    first.journal.append(FramePushed(
        run_id=first.run_id,
        frame_id="f0",
        attempt=0,
        depth=0,
        rig="budget",
        prompt="recover with a fresh attempt budget",
        branch=branch,
        base_commit=base,
        worktree=str(tmp_path / "lost-crash-worktree"),
        subtree_deadline=time.time() + 60,
    ))
    first.journal.append(FrameSlotAcquired(
        run_id=first.run_id,
        frame_id="f0",
        attempt=0,
        rig="budget",
    ))
    first.journal.append(FrameSlotReleased(
        run_id=first.run_id,
        frame_id="f0",
        attempt=0,
        rig="budget",
        active_s=rig.timeout_s,
        reason="engine_crash_fixture",
    ))

    resumed = Engine(
        run_dir,
        repo,
        registry,
        run_id="crash-budget",
        root_rig="budget",
        root_prompt="recover with a fresh attempt budget",
    )
    assert resumed.run().outcome == "ok"

    assert rig.runs == 1
    assert (repo / "crash-recovered.txt").read_text(encoding="utf-8") == "done\n"
    frame = resumed.projection.frame("f0")
    assert frame.attempt == 1
    assert frame.attempt_self_time_s[0] == rig.timeout_s
    assert 0 < frame.attempt_self_time_s[1] < rig.timeout_s
    replayed = Journal.load(run_dir).projection.frame("f0")
    assert replayed.attempt_self_time_s == frame.attempt_self_time_s
    assert replayed.self_time_s == frame.self_time_s
