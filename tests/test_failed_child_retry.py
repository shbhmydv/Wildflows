from __future__ import annotations

import json
from pathlib import Path
import threading

import pytest

from tests.conftest import executable
from wildflows.engine import Engine
from wildflows.events import (
    DispatchCalled,
    FrameExited,
    FramePopped,
    FramePushed,
)
from wildflows.frame import DispatchRequest, call_hash
from wildflows.rig import EchoRig, RigRegistry, ScriptRig


_RETRY_ADAPTER = r'''#!/usr/bin/env python3
import json
import os
import pathlib
import subprocess
import sys
import urllib.request

arguments = sys.argv[1:]
prompt_path = pathlib.Path(arguments[arguments.index("--prompt") + 1])
prompt = prompt_path.read_text(encoding="utf-8")
endpoint = os.environ["WILDFLOWS_MCP_URL"]
token = os.environ["WILDFLOWS_RUN_TOKEN"]
frame = os.environ["WILDFLOWS_FRAME_ID"]
mode = os.environ["RETRY_TEST_MODE"]
artifacts = pathlib.Path(os.environ["RETRY_ARTIFACTS"])
artifacts.mkdir(parents=True, exist_ok=True)


def call(index, arguments):
    request = urllib.request.Request(
        endpoint,
        data=json.dumps({
            "jsonrpc": "2.0", "id": index, "method": "tools/call",
            "params": {
                "name": "dispatch", "arguments": arguments,
                "_meta": {"wildflows": {"callIndex": index}},
            },
        }).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
            "X-Wildflows-Frame": frame,
        },
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)["result"]["structuredContent"]


cwd = pathlib.Path.cwd()
if frame == "f0":
    first = call(0, {
        "tasks": ["child task"], "rig": "script", "parallel": False,
    })
    (artifacts / "first.json").write_text(json.dumps(first))
    child_id = first["children"][0]["frame_id"]
    if mode == "retry":
        assert first["outcome"] == "failed"
        retried = call(1, {"retry_frame": child_id})
        (artifacts / "retry.json").write_text(json.dumps(retried))
        assert retried["outcome"] == "ok"
        assert (cwd / "prior-commit.txt").read_text() == "preserved\n"
        assert (cwd / "retry-finished.txt").read_text() == "finished\n"
    elif mode == "refuse":
        assert first["outcome"] == "ok"
        ok_retry = call(1, {"retry_frame": child_id})
        non_child = call(2, {"retry_frame": "f0.c99.t0"})
        (artifacts / "refusals.json").write_text(json.dumps({
            "ok": ok_retry, "non_child": non_child,
        }))
    else:
        assert first["outcome"] == "ok"
        assert (cwd / "ok-child.txt").read_text() == "integrated\n"
    print("root complete")
elif mode == "retry":
    counter_path = artifacts / "attempt-count"
    attempt = int(counter_path.read_text()) + 1 if counter_path.exists() else 1
    counter_path.write_text(str(attempt))
    if attempt == 1:
        (cwd / "prior-commit.txt").write_text("preserved\n")
        subprocess.run(["git", "add", "prior-commit.txt"], check=True)
        subprocess.run(["git", "commit", "-m", "preserve failed child work"], check=True)
        (cwd / "dirty-note.txt").write_text("uncommitted evidence\n")
        print("prior stdout evidence")
        print("prior stderr evidence", file=sys.stderr)
        raise SystemExit(7)
    assert (cwd / "prior-commit.txt").read_text() == "preserved\n"
    assert "--- EARLIER ATTEMPT ---" in prompt
    assert "This is relaunch attempt 2 for frame f0.c0.t0." in prompt
    assert "prior stdout evidence" in prompt
    assert "prior stderr evidence" in prompt
    assert "dirty-note.txt" in prompt
    assert "+uncommitted evidence" in prompt
    (artifacts / "retry-prompt.txt").write_text(prompt)
    (cwd / "retry-finished.txt").write_text("finished\n")
    print("retry complete")
else:
    (cwd / "ok-child.txt").write_text("integrated\n")
    print("ok child complete")
'''


def _engine(
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> Engine:
    adapter = executable(tmp_path / f"adapter-{mode}", _RETRY_ADAPTER)
    artifacts = tmp_path / f"artifacts-{mode}"
    monkeypatch.setenv("RETRY_TEST_MODE", mode)
    monkeypatch.setenv("RETRY_ARTIFACTS", str(artifacts))
    rig = ScriptRig(adapter, tmp_path / f"logs-{mode}", timeout_s=5)
    return Engine(
        tmp_path / f"run-{mode}",
        repo,
        RigRegistry({"script": rig}),
        run_id=f"failed-child-{mode}",
        root_rig="script",
        root_prompt="exercise child disposition",
        worktrees_root=tmp_path / f"worktrees-{mode}",
    )


def test_retry_request_is_exclusive_with_new_tasks() -> None:
    retry = DispatchRequest(retry_frame="f0.c0.t0")
    assert retry.tasks == []
    with pytest.raises(ValueError, match="retry_frame"):
        DispatchRequest(tasks=["new work"], retry_frame="f0.c0.t0")


def test_failed_result_surfaces_salvage_and_retry_reuses_branch_and_evidence(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "retry")

    assert engine.run().outcome == "ok"

    artifacts = tmp_path / "artifacts-retry"
    failed = json.loads((artifacts / "first.json").read_text(encoding="utf-8"))
    salvage = failed["children"][0]
    assert salvage["outcome"] == "failed"
    assert salvage["branch"] == "wildflows/failed-child-retry/f0.c0.t0"
    assert len(salvage["head"]) == 40
    assert "prior-commit.txt" in salvage["diffstat"]
    assert len(salvage["diffstat"].encode("utf-8")) <= 8192

    retried = json.loads((artifacts / "retry.json").read_text(encoding="utf-8"))
    assert retried["children"][0]["frame_id"] == salvage["frame_id"]
    child = engine.projection.frame(salvage["frame_id"])
    pushes = [
        event
        for event in engine.journal.events()
        if isinstance(event, FramePushed) and event.frame_id == child.frame_id
    ]
    assert len(pushes) == 2
    assert pushes[0].branch == pushes[1].branch == child.branch
    assert pushes[1].attempt == 1
    assert engine.repository.is_ancestor(salvage["head"], child.head or "")
    prompt = (artifacts / "retry-prompt.txt").read_text(encoding="utf-8")
    assert "Earlier attempt 1 died: failed" in prompt
    assert "dirty-note.txt" in prompt
    assert "+uncommitted evidence" in prompt
    retry_calls = [
        event
        for event in engine.journal.events()
        if isinstance(event, DispatchCalled) and event.request.retry_frame is not None
    ]
    assert [event.request.retry_frame for event in retry_calls] == [child.frame_id]


def test_pending_retry_replay_does_not_relaunch_an_already_failed_attempt(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = Engine(
        tmp_path / "run-retry-replay",
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id="retry-replay",
        root_rig="echo",
        root_prompt="replay pending retry",
        worktrees_root=tmp_path / "worktrees-retry-replay",
    )
    base = engine.repository.branch_tip()
    root_branch = engine.repository.frame_branch("f0")
    root_worktree = engine.repository.create_frame_worktree(
        "f0", root_branch, base, resume=False
    )
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id="f0",
        attempt=0,
        depth=0,
        rig="echo",
        prompt="replay pending retry",
        branch=root_branch,
        base_commit=base,
        worktree=str(root_worktree.path),
        subtree_deadline=4_102_444_800.0,
    ))
    child_id = "f0.c7.t0"
    child_branch = engine.repository.frame_branch(child_id)
    first_worktree = engine.repository.create_frame_worktree(
        child_id, child_branch, base, resume=False
    )
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=child_id,
        parent_frame_id="f0",
        parent_call_index=7,
        task_index=0,
        attempt=0,
        depth=1,
        rig="echo",
        prompt="retry me",
        branch=child_branch,
        base_commit=base,
        worktree=str(first_worktree.path),
        subtree_deadline=4_102_444_800.0,
    ))
    engine.journal.append(FrameExited(
        run_id=engine.run_id,
        frame_id=child_id,
        attempt=0,
        outcome="failed",
        text="first failure",
        head=base,
    ))
    engine.journal.append(FramePopped(
        run_id=engine.run_id,
        frame_id=child_id,
        attempt=0,
        outcome="failed",
    ))
    engine.repository.remove_worktree(first_worktree)

    request = DispatchRequest(retry_frame=child_id)
    digest = call_hash("dispatch", request)
    engine.journal.append(DispatchCalled(
        run_id=engine.run_id,
        frame_id="f0",
        call_index=0,
        call_hash=digest,
        request=request,
        caller_head=base,
    ))
    retry_worktree = engine.repository.create_frame_worktree(
        child_id, child_branch, base, resume=True
    )
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=child_id,
        parent_frame_id="f0",
        parent_call_index=7,
        task_index=0,
        attempt=1,
        depth=1,
        rig="echo",
        prompt="retry me",
        branch=child_branch,
        base_commit=base,
        worktree=str(retry_worktree.path),
        subtree_deadline=4_102_444_800.0,
    ))
    engine.journal.append(FrameExited(
        run_id=engine.run_id,
        frame_id=child_id,
        attempt=1,
        outcome="failed",
        text="retry also failed",
        head=base,
    ))
    engine.repository.remove_worktree(retry_worktree)

    def unexpected_launch(**kwargs: object) -> object:
        del kwargs
        raise AssertionError("completed retry attempt relaunched during replay")

    monkeypatch.setattr(engine, "_launch_frame", unexpected_launch)
    engine._call_context.cancellation = threading.Event()  # noqa: SLF001
    try:
        result = engine._dispatch_retry(  # noqa: SLF001 - pending retry replay seam
            engine.projection.frame("f0"),
            root_worktree,
            0,
            digest,
            request,
            True,
        )
    finally:
        del engine._call_context.cancellation  # noqa: SLF001
        engine.repository.remove_worktree(root_worktree)

    assert result.outcome == "failed"
    assert result.children[0].text == "retry also failed"
    pushes = [
        event
        for event in engine.journal.events()
        if isinstance(event, FramePushed) and event.frame_id == child_id
    ]
    assert len(pushes) == 2


def test_retry_refuses_non_child_and_successful_child(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "refuse")

    assert engine.run().outcome == "ok"
    refusals = json.loads(
        (tmp_path / "artifacts-refuse" / "refusals.json").read_text(encoding="utf-8")
    )
    assert refusals["ok"]["outcome"] == "refused"
    assert refusals["ok"]["error_code"] == "retry_child_not_failed"
    assert "failed direct child" in refusals["ok"]["message"]
    assert refusals["non_child"]["outcome"] == "refused"
    assert refusals["non_child"]["error_code"] == "retry_not_direct_child"
    assert "direct child" in refusals["non_child"]["message"]


def test_successful_child_still_auto_integrates(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "ok")

    assert engine.run().outcome == "ok"
    assert (repo / "ok-child.txt").read_text(encoding="utf-8") == "integrated\n"
    child = engine.projection.frame("f0.c0.t0")
    assert child.outcome == "ok"
    assert child.integrated is not None
