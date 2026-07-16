from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.conftest import executable
from wildflows.engine import Engine
from wildflows.events import (
    FrameSlotAcquired,
    FrameSlotReleased,
    GateCalled,
    GateReturned,
)
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
