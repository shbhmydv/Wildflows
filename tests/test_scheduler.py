from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from tests.conftest import executable
from wildflows.admission import AdmissionPolicy
from wildflows.engine import Engine
from wildflows.events import (
    FrameExited,
    FrameSlotAcquired,
    FrameSlotQueued,
    FrameSlotReleased,
    WorkerReaped,
)
from wildflows.rig import RigRegistry, ShellRig


_SCHEDULER_AGENT = r'''#!/usr/bin/env python3
import json
import os
import pathlib
import time
import urllib.request

endpoint = os.environ["WILDFLOWS_MCP_URL"]
token = os.environ["WILDFLOWS_RUN_TOKEN"]
frame = os.environ["WILDFLOWS_FRAME_ID"]
mode = os.environ["SCHEDULER_TEST_MODE"]


def call(index, name, arguments):
    request = urllib.request.Request(
        endpoint,
        data=json.dumps({
            "jsonrpc": "2.0", "id": index, "method": "tools/call",
            "params": {"name": name, "arguments": arguments,
                       "_meta": {"wildflows": {"callIndex": index}}},
        }).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + token,
                 "X-Wildflows-Frame": frame},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)["result"]


cwd = pathlib.Path.cwd()
if mode == "depth":
    if frame.count(".c") < 3:
        result = call(0, "dispatch", {
            "tasks": ["next nested frame"], "rig": "local", "parallel": False,
        })
        assert result["structuredContent"]["outcome"] == "ok"
    else:
        (cwd / "depth-three.txt").write_text("reached\n")
    print("depth complete")
elif mode == "queue":
    if frame == "f0":
        result = call(0, "dispatch", {
            "tasks": ["one", "two", "three"],
            "rig": "local", "parallel": True,
        })
        assert result["structuredContent"]["outcome"] == "ok"
        print("queue complete")
    else:
        time.sleep(0.18)
        index = frame.rsplit(".t", 1)[1]
        (cwd / ("child-" + index + ".txt")).write_text("done\n")
        print("child complete")
elif mode == "timeout":
    time.sleep(10)
elif mode == "affinity":
    (cwd / "provider.txt").write_text(os.environ["WILDFLOWS_PROVIDER_OVERRIDE"])
'''


def _engine(
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    *,
    timeout_s: float,
    slots: int,
) -> Engine:
    bin_dir = tmp_path / f"bin-{mode}"
    bin_dir.mkdir()
    executable(bin_dir / "scheduler-frame", _SCHEDULER_AGENT)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("SCHEDULER_TEST_MODE", mode)
    registry = RigRegistry(
        {"local": ShellRig("scheduler-frame", timeout_s=timeout_s)},
        slots={"local": slots},
    )
    return Engine(
        tmp_path / f"run-{mode}",
        repo,
        registry,
        run_id=f"scheduler-{mode}",
        root_rig="local",
        root_prompt=mode,
        policy=AdmissionPolicy(max_depth=4, subtree_timeout_s=10),
        worktrees_root=tmp_path / f"worktrees-{mode}",
    )


def test_slots_release_on_dispatch_park_and_reacquire_on_resume(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(
        repo, tmp_path, monkeypatch, "depth", timeout_s=2.0, slots=2
    )
    assert engine.run().outcome == "ok"
    assert (repo / "depth-three.txt").read_text(encoding="utf-8") == "reached\n"
    assert sorted(frame.depth for frame in engine.projection.frames.values()) == [
        0, 1, 2, 3,
    ]

    events = engine.journal.events()
    acquired = [event for event in events if isinstance(event, FrameSlotAcquired)]
    released = [event for event in events if isinstance(event, FrameSlotReleased)]
    assert len(acquired) == len(released) == 7
    for frame_id in {event.frame_id for event in acquired}:
        lanes = {
            event.slot for event in acquired
            if event.frame_id == frame_id and event.slot is not None
        }
        assert len(lanes) == 1
    for parent, child in zip(
        ("f0", "f0.c0.t0", "f0.c0.t0.c0.t0"),
        ("f0.c0.t0", "f0.c0.t0.c0.t0", "f0.c0.t0.c0.t0.c0.t0"),
        strict=True,
    ):
        parent_park = next(
            event.seq
            for event in released
            if event.frame_id == parent and event.reason == "dispatch"
        )
        child_start = next(
            event.seq for event in acquired if event.frame_id == child
        )
        child_exit = max(
            event.seq for event in released if event.frame_id == child
        )
        parent_resume = max(
            event.seq for event in acquired if event.frame_id == parent
        )
        assert parent_park < child_start < child_exit < parent_resume


def test_slot_queue_time_does_not_consume_frame_self_time(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    budget = 0.32
    engine = _engine(
        repo, tmp_path, monkeypatch, "queue", timeout_s=budget, slots=2
    )
    assert engine.run().outcome == "ok"

    queued = [
        event for event in engine.journal.events()
        if isinstance(event, FrameSlotQueued)
    ]
    assert queued
    queued_frame = engine.projection.frame(queued[0].frame_id)
    assert queued_frame.outcome == "ok"
    assert queued_frame.self_time_s < budget
    pushed_at = next(
        event.ts for event in engine.journal.events()
        if event.kind == "frame_pushed" and event.frame_id == queued_frame.frame_id
    )
    exited_at = next(
        event.ts for event in engine.journal.events()
        if isinstance(event, FrameExited) and event.frame_id == queued_frame.frame_id
    )
    assert exited_at - pushed_at > budget


def test_self_time_exhaustion_reaps_worker_and_is_journalled(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(
        repo, tmp_path, monkeypatch, "timeout", timeout_s=0.15, slots=1
    )
    started = time.monotonic()
    result = engine.run()
    elapsed = time.monotonic() - started

    assert result.outcome == "failed"
    assert elapsed < 1.0
    reaped = [
        event for event in engine.journal.events()
        if isinstance(event, WorkerReaped)
    ]
    assert reaped and reaped[-1].reason == "frame_self_timeout"
    exits = [
        event for event in engine.journal.events()
        if isinstance(event, FrameExited)
    ]
    assert exits[-1].outcome == "failed"
    assert "self-time budget" in exits[-1].text


def test_assigned_slot_provider_override_reaches_adapter(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(
        repo, tmp_path, monkeypatch, "affinity", timeout_s=1.0, slots=2
    )
    assert engine.run().outcome == "ok"
    assert (repo / "provider.txt").read_text(encoding="utf-8") in {
        "local-reviewer-8081",
        "local-reviewer-8082",
    }
