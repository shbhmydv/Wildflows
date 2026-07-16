from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import cast
import urllib.request

import pytest

from wildflows.engine import Engine
from wildflows.events import (
    CallFailed,
    CallRefused,
    DispatchCalled,
    GateCalled,
)
from wildflows.frame import (
    DispatchRequest,
    FrameResult,
    FrameRuntime,
    GateRequest,
    ToolName,
    call_hash,
)
from wildflows.rig import RigRegistry


class _RefusalInterrupted(BaseException):
    pass


def _tool_call(
    runtime: FrameRuntime,
    index: int,
    tool: ToolName,
    arguments: dict[str, object],
) -> dict[str, object]:
    payload = {
        "jsonrpc": "2.0",
        "id": index,
        "method": "tools/call",
        "params": {
            "name": tool,
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
    return cast(dict[str, object], decoded)


def _structured(response: dict[str, object]) -> dict[str, object]:
    result = response["result"]
    assert isinstance(result, dict)
    structured = result["structuredContent"]
    assert isinstance(structured, dict)
    return cast(dict[str, object], structured)


@dataclass
class _DirtyThenRetryRig:
    tool: ToolName
    run_dir: Path
    timeout_s: float = 10.0
    refusal_was_immediate: bool = False
    refused_payload: dict[str, object] | None = None

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt
        if runtime.frame_id != Engine.ROOT_FRAME_ID:
            return FrameResult(text="dispatch child complete", exit_code=0)

        dirty = workdir / "dirty.txt"
        dirty.write_text("uncommitted\n", encoding="utf-8")
        arguments: dict[str, object]
        if self.tool == "dispatch":
            arguments = {
                "tasks": ["clean child"],
                "rig": "worker",
                "parallel": False,
            }
        else:
            arguments = {"cmd": "printf clean-gate"}
        refused = _tool_call(runtime, 0, self.tool, arguments)
        self.refused_payload = _structured(refused)
        records = [
            json.loads(line)
            for line in (self.run_dir / "events.ndjson")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.refusal_was_immediate = any(
            record["kind"] == "call_refused" and record["call_index"] == 0
            for record in records
        ) and not any(
            record["kind"] in ("dispatch_called", "gate_called")
            and record["call_index"] == 0
            for record in records
        )

        dirty.unlink()
        succeeded = _structured(_tool_call(runtime, 1, self.tool, arguments))
        if self.tool == "dispatch":
            assert succeeded["outcome"] == "ok"
        else:
            assert succeeded == {
                "exit_code": 0,
                "stdout": "clean-gate",
                "stderr": "",
            }
        return FrameResult(text="cleaned and retried", exit_code=0)


@pytest.mark.parametrize("tool", ["dispatch", "gate"])
def test_dirty_call_is_durably_refused_then_clean_retry_succeeds(
    repo: Path,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    tool: ToolName,
) -> None:
    run_dir = tmp_path / f"run-{tool}"
    rig = _DirtyThenRetryRig(tool, run_dir)
    engine = Engine(
        run_dir,
        repo,
        RigRegistry({"worker": rig}),
        run_id=f"dirty-{tool}",
        root_rig="worker",
        root_prompt=f"refuse and retry {tool}",
        worktrees_root=tmp_path / f"worktrees-{tool}",
    )
    caplog.set_level(logging.ERROR, logger="wildflows.engine")

    assert engine.run().outcome == "ok"

    assert rig.refusal_was_immediate
    assert rig.refused_payload is not None
    reason = rig.refused_payload["message"]
    assert isinstance(reason, str)
    assert rig.refused_payload == {
        "outcome": "failed",
        "error_code": "call_refused",
        "message": reason,
    }
    assert "git status --porcelain" in reason
    assert "?? dirty.txt" in reason
    assert "commit or clean" in reason
    assert "then retry" in reason

    refusals = [
        event for event in engine.journal.events() if isinstance(event, CallRefused)
    ]
    assert len(refusals) == 1
    refusal = refusals[0]
    assert refusal.frame_id == "f0"
    assert refusal.call_index == 0
    assert refusal.tool == tool
    assert refusal.reason == reason
    expected_request = (
        DispatchRequest(tasks=["clean child"], rig="worker", parallel=False)
        if tool == "dispatch"
        else GateRequest(cmd="printf clean-gate")
    )
    assert refusal.request == expected_request
    assert refusal.call_hash == call_hash(tool, expected_request)
    assert not any(isinstance(event, CallFailed) for event in engine.journal.events())
    called_type = DispatchCalled if tool == "dispatch" else GateCalled
    called = [event for event in engine.journal.events() if isinstance(event, called_type)]
    assert len(called) == 1
    assert called[0].call_index == 1
    assert called[0].seq > refusal.seq
    assert any(record.getMessage().endswith(reason) for record in caplog.records)


@dataclass
class _InterruptAfterRefusalRig:
    timeout_s: float = 10.0

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt
        assert runtime.next_call_index == 0
        (workdir / "dirty.txt").write_text("uncommitted\n", encoding="utf-8")
        refused = _structured(
            _tool_call(runtime, 0, "gate", {"cmd": "printf resumed"})
        )
        assert refused["error_code"] == "call_refused"
        raise _RefusalInterrupted()


@dataclass
class _ResumeAfterRefusalRig:
    timeout_s: float = 10.0
    observed_next_index: int | None = None

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, workdir
        self.observed_next_index = runtime.next_call_index
        result = _structured(
            _tool_call(runtime, runtime.next_call_index, "gate", {"cmd": "printf resumed"})
        )
        assert result == {"exit_code": 0, "stdout": "resumed", "stderr": ""}
        return FrameResult(text="refusal replayed and retry succeeded", exit_code=0)


def test_resume_replays_refusal_and_allows_same_tool_at_next_index(
    repo: Path, tmp_path: Path
) -> None:
    run_dir = tmp_path / "run-resume-refusal"
    first = Engine(
        run_dir,
        repo,
        RigRegistry({"worker": _InterruptAfterRefusalRig()}),
        run_id="resume-refusal",
        root_rig="worker",
        root_prompt="resume after pre-journal refusal",
        worktrees_root=tmp_path / "worktrees-resume-refusal",
    )
    with pytest.raises(_RefusalInterrupted):
        first.run()
    assert len([
        event for event in first.journal.events() if isinstance(event, CallRefused)
    ]) == 1
    assert first.projection.call("f0", 0) is None

    rig = _ResumeAfterRefusalRig()
    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"worker": rig}),
        run_id="resume-refusal",
        root_rig="worker",
        root_prompt="resume after pre-journal refusal",
    )
    assert resumed.run().outcome == "ok"
    assert rig.observed_next_index == 1
    events = resumed.journal.events()
    refusal = next(event for event in events if isinstance(event, CallRefused))
    called = next(event for event in events if isinstance(event, GateCalled))
    assert refusal.call_index == 0
    assert called.call_index == 1
    assert refusal.seq < called.seq
