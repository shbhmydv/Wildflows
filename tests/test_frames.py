from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from wildflows.admission import AdmissionPolicy
from wildflows.engine import Engine
from wildflows.events import (
    Answered,
    DispatchReturned,
    FramePushed,
    GateReturned,
)
from wildflows.frame import AskRequest
from wildflows.rig import RigRegistry, ShellRig
from tests.conftest import executable


_FAKE_AGENT = r'''#!/usr/bin/env python3
import json
import os
import pathlib
import threading
import time
import urllib.request

endpoint = os.environ["WILDFLOWS_MCP_URL"]
token = os.environ["WILDFLOWS_RUN_TOKEN"]
frame = os.environ["WILDFLOWS_FRAME_ID"]
mode = os.environ.get("FRAME_TEST_MODE", "depth")

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
    if frame == "f0":
        call(0, "dispatch", {"tasks": ["child"], "rig": "fake", "parallel": False})
        assert (cwd / "grand.txt").read_text() == "grand\n"
        assert (cwd / "child.txt").read_text() == "child\n"
        gate = call(1, "gate", {"cmd": "python3 -c 'import sys;print(\"OUT\");print(\"ERR\",file=sys.stderr);raise SystemExit(3)'"})
        data = gate["structuredContent"]
        assert data["exit_code"] == 3 and data["stdout"] == "OUT\n" and data["stderr"] == "ERR\n"
        (cwd / "root.txt").write_text("root\n")
        print("root complete")
    elif frame.count(".c") == 1:
        call(0, "dispatch", {"tasks": ["grand"], "rig": "fake", "parallel": False})
        assert (cwd / "grand.txt").read_text() == "grand\n"
        (cwd / "child.txt").write_text("child\n")
        print("child complete")
    else:
        (cwd / "grand.txt").write_text("grand\n")
        print("grand complete")
elif mode in ("parallel", "conflict"):
    if frame == "f0":
        result = call(0, "dispatch", {"tasks": ["left", "right"], "rig": "fake", "parallel": True})
        (cwd / "dispatch-result.json").write_text(json.dumps(result["structuredContent"]))
        print("parallel root complete")
    else:
        index = int(frame.rsplit(".t", 1)[1])
        barrier = pathlib.Path(os.environ["FRAME_BARRIER_DIR"])
        barrier.mkdir(parents=True, exist_ok=True)
        (barrier / str(index)).write_text("started")
        deadline = time.monotonic() + 5
        while len(list(barrier.iterdir())) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(list(barrier.iterdir())) >= 2, "parallel siblings did not overlap"
        name = "shared.txt" if mode == "conflict" else ("left.txt" if index == 0 else "right.txt")
        (cwd / name).write_text(str(index) + "\n")
        print("parallel child", index)
elif mode == "singleflight":
    if frame == "f0":
        results = []
        def gate():
            results.append(call(0, "gate", {"cmd": "echo run >> gate-count.txt; sleep 0.2"}))
        threads = [threading.Thread(target=gate), threading.Thread(target=gate)]
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]
        assert len(results) == 2
        print("single flight complete")
elif mode == "kill-resume":
    if frame == "f0":
        call(0, "dispatch", {"tasks": ["paid child"], "rig": "fake", "parallel": False})
        pathlib.Path(os.environ["ROOT_MARKER"]).write_text(str(os.getpid()))
        release = pathlib.Path(os.environ["ROOT_RELEASE"])
        while not release.exists():
            time.sleep(0.05)
        assert (cwd / "child.txt").read_text() == "paid once\n"
        (cwd / "root.txt").write_text("resumed\n")
        print("kill resume root complete")
    else:
        counter = pathlib.Path(os.environ["CHILD_COUNTER"])
        count = int(counter.read_text()) if counter.exists() else 0
        counter.write_text(str(count + 1))
        (cwd / "child.txt").write_text("paid once\n")
        print("paid child complete")
elif mode in ("rail-frames", "rail-spend"):
    if frame == "f0":
        call(0, "dispatch", {"tasks": ["nested rail child"], "rig": "fake", "parallel": False})
        print("rail root complete")
    elif frame.count(".c") == 1:
        result = call(0, "dispatch", {"tasks": ["grand 1", "grand 2"], "rig": "fake", "parallel": False})
        (cwd / "nested-admission.json").write_text(json.dumps(result["structuredContent"]))
        print("nested refusal observed")
    else:
        raise SystemExit("nested rail unexpectedly launched a grandchild")
elif mode == "admission":
    if frame == "f0":
        result = call(0, "dispatch", {"tasks": ["denied"], "rig": "fake", "parallel": False})
        (cwd / "admission.json").write_text(json.dumps(result["structuredContent"]))
        print("admission observed")
    else:
        raise SystemExit("refused dispatch unexpectedly launched a child")
elif mode == "ask":
    if frame == "f0":
        result = call(0, "ask", {"question": "ship it?"})
        (cwd / "answer.txt").write_text(result["structuredContent"]["answer"])
        print("answer received")
'''


def _engine(
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    *,
    max_depth: int = 4,
    max_subtree_frames: int = 64,
    max_subtree_spend: float = 64.0,
) -> Engine:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    executable(bin_dir / "fake-frame", _FAKE_AGENT)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FRAME_TEST_MODE", mode)
    monkeypatch.setenv("FRAME_BARRIER_DIR", str(tmp_path / f"barrier-{mode}"))
    registry = RigRegistry({"fake": ShellRig("fake-frame", timeout_s=30)})
    return Engine(
        tmp_path / f"run-{mode}",
        repo,
        registry,
        run_id=f"test-{mode}",
        root_rig="fake",
        root_prompt="root job",
        policy=AdmissionPolicy(
            max_depth=max_depth,
            max_subtree_frames=max_subtree_frames,
            max_subtree_spend=max_subtree_spend,
        ),
        worktrees_root=tmp_path / f"worktrees-{mode}",
    )


def test_depth_two_stack_integrates_up_and_gate_journals_both_streams(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "depth")
    result = engine.run()

    assert result.outcome == "ok"
    assert (repo / "grand.txt").read_text(encoding="utf-8") == "grand\n"
    assert (repo / "child.txt").read_text(encoding="utf-8") == "child\n"
    assert (repo / "root.txt").read_text(encoding="utf-8") == "root\n"
    gates = [event for event in engine.journal.events() if isinstance(event, GateReturned)]
    assert len(gates) == 1
    assert gates[0].result.model_dump() == {
        "exit_code": 3, "stdout": "OUT\n", "stderr": "ERR\n"
    }
    pushes = [event for event in engine.journal.events() if isinstance(event, FramePushed)]
    assert [event.depth for event in pushes] == [0, 1, 2]
    assert all(not Path(event.worktree).is_relative_to(repo) for event in pushes)


def test_dispatch_admission_refusal_is_typed_and_has_no_child(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "admission", max_depth=0)
    assert engine.run().outcome == "ok"
    data = json.loads((repo / "admission.json").read_text(encoding="utf-8"))
    assert data["outcome"] == "refused"
    assert data["error_code"] == "depth_cap"
    assert len(engine.projection.frames) == 1
    returned = [
        event for event in engine.journal.events() if isinstance(event, DispatchReturned)
    ]
    assert returned[0].result.outcome == "refused"


@pytest.mark.parametrize(
    ("mode", "frame_cap", "spend_cap", "code"),
    [
        ("rail-frames", 2, 64.0, "subtree_frame_cap"),
        ("rail-spend", 64, 2.0, "subtree_spend_cap"),
    ],
)
def test_nested_dispatch_is_charged_to_every_ancestor_subtree(
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    frame_cap: int,
    spend_cap: float,
    code: str,
) -> None:
    engine = _engine(
        repo,
        tmp_path,
        monkeypatch,
        mode,
        max_subtree_frames=frame_cap,
        max_subtree_spend=spend_cap,
    )
    assert engine.run().outcome == "ok"
    payload = json.loads(
        (repo / "nested-admission.json").read_text(encoding="utf-8")
    )
    assert payload["outcome"] == "refused"
    assert payload["error_code"] == code
    assert len(engine.projection.frames) == 2


def test_duplicate_live_call_is_single_flight_and_memoized(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "singleflight")
    assert engine.run().outcome == "ok"
    assert (repo / "gate-count.txt").read_text(encoding="utf-8") == "run\n"
    gates = [event for event in engine.journal.events() if isinstance(event, GateReturned)]
    assert len(gates) == 1


def test_ask_parks_until_owner_answer(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "ask")
    result_box: list[object] = []

    import threading

    thread = threading.Thread(target=lambda: result_box.append(engine.run()))
    thread.start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not engine.projection.pending_questions():
        time.sleep(0.02)
    pending = engine.projection.pending_questions()[0].request
    assert isinstance(pending, AskRequest)
    assert pending.question == "ship it?"
    engine.answer("yes")
    with pytest.raises(ValueError):
        engine.answer("no")
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert (repo / "answer.txt").read_text(encoding="utf-8") == "yes"
    assert any(isinstance(event, Answered) for event in engine.journal.events())
