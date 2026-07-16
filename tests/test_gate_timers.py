from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.conftest import executable
from wildflows.engine import Engine
from wildflows.events import (
    FramePushed,
    FrameSlotAcquired,
    FrameSlotReleased,
    GateCalled,
    GateReturned,
)
from wildflows.frame import GateRequest, GateResult, call_hash
from wildflows.journal import Journal
from wildflows.rig import RigRegistry, ShellRig


_GATE_AGENT = r'''#!/usr/bin/env python3
import json
import os
import pathlib
import urllib.request

endpoint = os.environ["WILDFLOWS_MCP_URL"]
token = os.environ["WILDFLOWS_RUN_TOKEN"]
frame = os.environ["WILDFLOWS_FRAME_ID"]
command = os.environ["GATE_TEST_COMMAND"]

request = urllib.request.Request(
    endpoint,
    data=json.dumps({
        "jsonrpc": "2.0", "id": 0, "method": "tools/call",
        "params": {
            "name": "gate", "arguments": {"cmd": command},
            "_meta": {"wildflows": {"callIndex": 0}},
        },
    }).encode(),
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer " + token,
        "X-Wildflows-Frame": frame,
    },
)
with urllib.request.urlopen(request, timeout=10) as response:
    result = json.load(response)["result"]["structuredContent"]
pathlib.Path("gate-result.json").write_text(json.dumps(result))
print("frame survived gate")
'''


def _engine(
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    command: str,
    timeout_s: float,
    gate_timeout_s: float | None = None,
) -> Engine:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    executable(bin_dir / "gate-agent", _GATE_AGENT)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("GATE_TEST_COMMAND", command)
    registry = RigRegistry(
        {"gate": ShellRig("gate-agent", timeout_s=timeout_s)},
        gate_timeouts={"gate": gate_timeout_s} if gate_timeout_s is not None else None,
        slots={"gate": 1},
    )
    return Engine(
        tmp_path / "run",
        repo,
        registry,
        run_id="gate-timer",
        root_rig="gate",
        root_prompt="exercise gate timer",
        worktrees_root=tmp_path / "worktrees",
    )


def test_slow_gate_pauses_self_time_and_replays_identically(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    budget_s = 0.16
    engine = _engine(
        repo,
        tmp_path,
        monkeypatch,
        command="sleep 0.3; printf gate-finished",
        timeout_s=budget_s,
    )

    result = engine.run()

    assert result.outcome == "ok"
    gate_result = json.loads((repo / "gate-result.json").read_text(encoding="utf-8"))
    assert gate_result == {
        "exit_code": 0,
        "stdout": "gate-finished",
        "stderr": "",
    }
    live_self_time = engine.projection.frame("f0").self_time_s
    assert 0 < live_self_time < budget_s
    replayed = Journal.load(tmp_path / "run").projection.frame("f0")
    assert replayed.self_time_s == live_self_time

    events = engine.journal.events()
    acquired = next(event for event in events if isinstance(event, FrameSlotAcquired))
    called = next(event for event in events if isinstance(event, GateCalled))
    returned = next(event for event in events if isinstance(event, GateReturned))
    released = next(event for event in events if isinstance(event, FrameSlotReleased))
    assert acquired.seq < called.seq < returned.seq < released.seq
    assert released.reason == "exit"
    assert released.slot == acquired.slot == 0


def test_resume_uses_gate_timestamps_to_exclude_an_orphaned_wait(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = RigRegistry({"gate": ShellRig("true", timeout_s=30)})
    run_dir = tmp_path / "orphan-run"
    initial = Engine(
        run_dir,
        repo,
        registry,
        run_id="orphan-gate",
        root_rig="gate",
        root_prompt="resume an interrupted gate",
        worktrees_root=tmp_path / "orphan-worktrees",
    )
    base = initial.repository.branch_tip()
    request = GateRequest(cmd="sleep forever")
    digest = call_hash("gate", request)
    initial.journal.append(FramePushed(
        run_id=initial.run_id,
        frame_id="f0",
        attempt=0,
        depth=0,
        rig="gate",
        prompt="resume an interrupted gate",
        branch=initial.repository.frame_branch("f0"),
        base_commit=base,
        worktree=str(tmp_path / "interrupted-worktree"),
        subtree_deadline=200.0,
        ts=100.0,
    ))
    initial.journal.append(FrameSlotAcquired(
        run_id=initial.run_id,
        frame_id="f0",
        attempt=0,
        rig="gate",
        ts=101.0,
    ))
    initial.journal.append(GateCalled(
        run_id=initial.run_id,
        frame_id="f0",
        call_index=0,
        call_hash=digest,
        request=request,
        caller_head=base,
        ts=103.0,
    ))
    initial.journal.append(GateReturned(
        run_id=initial.run_id,
        frame_id="f0",
        call_index=0,
        call_hash=digest,
        result=GateResult(exit_code=0, stdout="", stderr=""),
        ts=110.0,
    ))

    monkeypatch.setattr("wildflows.engine.time.time", lambda: 113.0)
    resumed = Engine(
        run_dir,
        repo,
        registry,
        run_id="orphan-gate",
        root_rig="gate",
        root_prompt="resume an interrupted gate",
    )

    release = next(
        event
        for event in resumed.journal.events()
        if isinstance(event, FrameSlotReleased)
    )
    assert release.reason == "engine_resume_sweep"
    assert release.active_s == 5.0  # 12s leased minus the journalled 7s gate wait.
    assert resumed.projection.frame("f0").self_time_s == 5.0
    assert Journal.load(run_dir).projection.frame("f0").self_time_s == 5.0


def test_configured_gate_timeout_returns_124_without_killing_frame(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(
        repo,
        tmp_path,
        monkeypatch,
        command="sleep 0.4",
        timeout_s=1.0,
        gate_timeout_s=0.08,
    )

    assert engine.run().outcome == "ok"
    gate_result = json.loads((repo / "gate-result.json").read_text(encoding="utf-8"))
    assert gate_result["exit_code"] == 124
    assert gate_result["stdout"] == ""
    assert "[timeout] gate exceeded 0.08s" in gate_result["stderr"]


def test_unset_gate_timeout_is_unbounded_by_wildflows(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(
        repo,
        tmp_path,
        monkeypatch,
        command="sleep 0.12; printf unbounded",
        timeout_s=0.5,
    )

    assert engine.run().outcome == "ok"
    gate_result = json.loads((repo / "gate-result.json").read_text(encoding="utf-8"))
    assert gate_result["exit_code"] == 0
    assert gate_result["stdout"] == "unbounded"
