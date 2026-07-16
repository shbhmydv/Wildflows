from __future__ import annotations

import os
import shlex
from pathlib import Path

import pytest

from tests.conftest import executable, git
from wildflows.engine import Engine
from wildflows.events import FrameExited, WorktreeProvisioned
from wildflows.rig import RigRegistry, ShellRig


_PROVISION_AGENT = r'''#!/usr/bin/env python3
import json
import os
import pathlib
import urllib.request

endpoint = os.environ["WILDFLOWS_MCP_URL"]
token = os.environ["WILDFLOWS_RUN_TOKEN"]
frame = os.environ["WILDFLOWS_FRAME_ID"]
mode = os.environ["PROVISION_TEST_MODE"]


def call(index, name, arguments):
    request = urllib.request.Request(
        endpoint,
        data=json.dumps({
            "jsonrpc": "2.0", "id": index, "method": "tools/call",
            "params": {
                "name": name, "arguments": arguments,
                "_meta": {"wildflows": {"callIndex": index}},
            },
        }).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
            "X-Wildflows-Frame": frame,
        },
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)["result"]["structuredContent"]


if mode == "setup":
    gate = call(0, "gate", {"cmd": "test -f .deps/ready"})
    assert gate["exit_code"] == 0
    if frame == "f0":
        dispatched = call(1, "dispatch", {
            "tasks": ["verify provisioned child"],
            "rig": "worker", "parallel": False,
        })
        assert dispatched["outcome"] == "ok"
    print("setup visible")
elif mode == "link":
    gate = call(0, "gate", {
        "cmd": "test -L .shared/cache && "
               "test \"$(cat .shared/cache/value.txt)\" = shared && "
               "test ! -e .shared/missing",
    })
    assert gate["exit_code"] == 0
    print("links visible")
else:
    pathlib.Path(os.environ["AGENT_STARTED"]).write_text("started\n")
    raise SystemExit("failed setup unexpectedly launched the frame adapter")
'''


def _registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str,
    setup: str | None = None,
    links: list[str] | None = None,
) -> RigRegistry:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    executable(bin_dir / "provision-agent", _PROVISION_AGENT)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("PROVISION_TEST_MODE", mode)
    monkeypatch.setenv("AGENT_STARTED", str(tmp_path / "agent-started"))
    return RigRegistry(
        {"worker": ShellRig("provision-agent", timeout_s=2)},
        worktree_setup=setup,
        worktree_links=links,
    )


def _engine(
    repo: Path,
    tmp_path: Path,
    registry: RigRegistry,
    *,
    run_id: str,
) -> Engine:
    return Engine(
        tmp_path / f"run-{run_id}",
        repo,
        registry,
        run_id=run_id,
        root_rig="worker",
        root_prompt="verify worktree provisioning",
        worktrees_root=tmp_path / f"worktrees-{run_id}",
    )


def test_setup_runs_once_per_new_worktree_is_visible_to_gates_and_not_replayed(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (repo / ".gitignore").write_text(".deps/\n", encoding="utf-8")
    git(repo, "add", ".gitignore")
    git(repo, "commit", "-m", "ignore provisioned dependency data")
    counter = tmp_path / "setup-count"
    setup = (
        "mkdir -p .deps; printf ready > .deps/ready; "
        f"printf '%s\\n' \"$PWD\" >> {shlex.quote(str(counter))}"
    )
    registry = _registry(
        tmp_path, monkeypatch, mode="setup", setup=setup
    )
    engine = _engine(repo, tmp_path, registry, run_id="setup-once")

    assert engine.run().outcome == "ok"
    provisioned = [
        event
        for event in engine.journal.events()
        if isinstance(event, WorktreeProvisioned)
        and event.mechanism == "setup"
    ]
    assert len(provisioned) == 2
    assert all(event.outcome == "ok" and event.duration_s >= 0 for event in provisioned)
    created_paths = counter.read_text(encoding="utf-8").splitlines()
    assert len(created_paths) == len(set(created_paths)) == 2

    resumed = _engine(repo, tmp_path, registry, run_id="setup-once")
    assert resumed.run().outcome == "ok"
    assert counter.read_text(encoding="utf-8").splitlines() == created_paths
    assert len([
        event
        for event in resumed.journal.events()
        if isinstance(event, WorktreeProvisioned)
    ]) == 2


def test_setup_failure_terminalizes_launch_and_removes_worktree(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = _registry(
        tmp_path,
        monkeypatch,
        mode="failure",
        setup="printf setup-out; printf setup-err >&2; exit 9",
    )
    engine = _engine(repo, tmp_path, registry, run_id="setup-failure")

    result = engine.run()

    assert result.outcome == "failed"
    assert "worktree setup failed with exit 9" in result.text
    assert "setup-out" in result.text
    assert "setup-err" in result.text
    assert not (tmp_path / "agent-started").exists()
    event = next(
        event
        for event in engine.journal.events()
        if isinstance(event, WorktreeProvisioned)
    )
    assert event.mechanism == "setup"
    assert event.outcome == "failed"
    assert "setup-out" in event.output_tail
    assert "setup-err" in event.output_tail
    assert any(isinstance(event, FrameExited) for event in engine.journal.events())
    assert not list((tmp_path / "worktrees-setup-failure").glob("*"))
    worktree_listing = git(repo, "worktree", "list", "--porcelain")
    assert worktree_listing.count("worktree ") == 1


def test_links_share_existing_sources_and_warn_for_missing_sources(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (repo / ".gitignore").write_text(".shared/\n", encoding="utf-8")
    git(repo, "add", ".gitignore")
    git(repo, "commit", "-m", "ignore shared cache data")
    cache = repo / ".shared" / "cache"
    cache.mkdir(parents=True)
    (cache / "value.txt").write_text("shared\n", encoding="utf-8")
    registry = _registry(
        tmp_path,
        monkeypatch,
        mode="link",
        links=[".shared/cache", ".shared/missing"],
    )
    engine = _engine(repo, tmp_path, registry, run_id="links")

    assert engine.run().outcome == "ok"
    event = next(
        event
        for event in engine.journal.events()
        if isinstance(event, WorktreeProvisioned)
    )
    assert event.mechanism == "link"
    assert event.outcome == "ok"
    assert event.linked == [".shared/cache"]
    assert event.warnings == [
        "worktree link source does not exist; skipped: .shared/missing"
    ]
