from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import executable, git
from tests.test_frames import _FAKE_AGENT


@pytest.mark.skipif(not Path("/proc").is_dir(), reason="requires Linux parent-death proof")
def test_sigkill_resume_memoizes_child_and_run_branch_moves_only_at_root_pop(
    repo: Path, tmp_path: Path
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    executable(bin_dir / "fake-frame", _FAKE_AGENT)
    job = tmp_path / "job.md"
    job.write_text("kill replay job\n", encoding="utf-8")
    rigs = tmp_path / "rigs.yaml"
    rigs.write_text(
        """rigs:
  fake:
    kind: shell
    template: fake-frame
    timeout_s: 30
""",
        encoding="utf-8",
    )
    marker = tmp_path / "root-marker"
    release = tmp_path / "root-release"
    counter = tmp_path / "child-count"
    worktrees = tmp_path / "external-worktrees"
    environment = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "FRAME_TEST_MODE": "kill-resume",
        "ROOT_MARKER": str(marker),
        "ROOT_RELEASE": str(release),
        "CHILD_COUNTER": str(counter),
        "FRAME_BARRIER_DIR": str(tmp_path / "unused-barrier"),
    }
    command = [
        sys.executable,
        "-m",
        "wildflows",
        "run",
        str(job),
        "--repo",
        str(repo),
        "--rigs",
        str(rigs),
        "--root-rig",
        "fake",
        "--run-id",
        "killed",
        "--worktrees-root",
        str(worktrees),
    ]
    initial = git(repo, "rev-parse", "main")
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=environment
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not marker.exists():
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            pytest.fail(f"run exited before kill point: {stdout}\n{stderr}")
        time.sleep(0.02)
    assert marker.exists()
    root_agent_pid = int(marker.read_text(encoding="utf-8"))
    assert counter.read_text(encoding="utf-8") == "1"
    # The child is already in f0's branch, but the owner-selected run branch has
    # not moved while the resident root is still alive.
    assert git(repo, "rev-parse", "main") == initial

    process.kill()
    process.wait(timeout=10)
    def live(pid: int) -> bool:
        try:
            state = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()[2]
            return state != "Z"
        except FileNotFoundError:
            return False

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and live(root_agent_pid):
        time.sleep(0.02)
    assert not live(root_agent_pid)
    release.write_text("resume", encoding="utf-8")

    resumed_command = [*command]
    resumed_command[3] = "resume"
    resumed = subprocess.run(
        resumed_command, capture_output=True, text=True, env=environment, timeout=30
    )
    assert resumed.returncode == 0, resumed.stderr
    assert json.loads(resumed.stdout.splitlines()[-1])["outcome"] == "ok"
    assert counter.read_text(encoding="utf-8") == "1"
    assert (repo / "child.txt").read_text(encoding="utf-8") == "paid once\n"
    assert (repo / "root.txt").read_text(encoding="utf-8") == "resumed\n"
    assert git(repo, "rev-parse", "main") != initial

    events = (repo / ".wildflows" / "runs" / "killed" / "events.ndjson").read_text()
    records = [json.loads(line) for line in events.splitlines()]
    assert len([event for event in records if event["kind"] == "dispatch_called"]) == 1
    assert len([event for event in records if event["kind"] == "dispatch_returned"]) == 1
    pushed = [Path(event["worktree"]) for event in records if event["kind"] == "frame_pushed"]
    assert pushed and all(path.is_relative_to(worktrees) for path in pushed)
    assert all(not path.is_relative_to(repo) for path in pushed)
