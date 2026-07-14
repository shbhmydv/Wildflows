from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from tests.conftest import executable
from wildflows.frame import FrameRuntime
from wildflows.rig import ScriptRig
from wildflows.rigconfig import load_rigs
from wildflows.shim import write_pi_shim


def test_pi_shim_is_private_and_outside_worktree(tmp_path: Path) -> None:
    worktree = tmp_path / "repo"
    worktree.mkdir()
    runtime = tmp_path / "run" / "runtime" / "f0"
    shim = write_pi_shim(
        runtime, "http://127.0.0.1:1234/mcp", "secret-token", "f0", 3
    )
    assert not shim.is_relative_to(worktree)
    assert shim.stat().st_mode & 0o777 == 0o600
    source = shim.read_text(encoding="utf-8")
    assert 'const endpoint = "http://127.0.0.1:1234/mcp"' in source
    assert 'const token = "secret-token"' in source
    assert "let nextCallIndex = 3" in source
    assert "wildflows_dispatch" in source
    assert "wildflows_gate" in source
    assert "wildflows_ask" in source


def test_pi_shim_carries_replay_call_identity(tmp_path: Path) -> None:
    shim = write_pi_shim(
        tmp_path / "runtime",
        "http://127.0.0.1:1/mcp",
        "token",
        "f0",
        2,
        [(0, "gate", {"cmd": "true"}), (1, "ask", {"question": "ship?"})],
    )
    source = shim.read_text(encoding="utf-8")
    assert '"callIndex": 0' in source
    assert '"name": "gate"' in source
    assert "allocateCallIndex" in source
    assert "claimedReplayCalls" in source


def test_script_rig_passes_frame_capability_out_of_band(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    script = executable(
        tmp_path / "adapter",
        """#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
pathlib.Path(os.environ['CAPTURE']).write_text('|'.join(args) + '\\n' +
    os.environ['WILDFLOWS_MCP_URL'] + '\\n' +
    os.environ['WILDFLOWS_RUN_TOKEN'] + '\\n' +
    os.environ['WILDFLOWS_FRAME_ID'] + '\\n' +
    os.environ['WILDFLOWS_PI_EXTENSION'])
print('adapter report')
""",
    )
    capture = tmp_path / "capture"
    shim = tmp_path / "shim.ts"
    shim.write_text("shim", encoding="utf-8")
    rig = ScriptRig(
        script,
        tmp_path / "logs",
        timeout_s=10,
        env={"CAPTURE": str(capture)},
    )
    runtime = FrameRuntime(
        endpoint="http://127.0.0.1:1/mcp",
        token="token",
        frame_id="f0",
        shim_path=shim,
        runtime_dir=tmp_path / "runtime",
        next_call_index=0,
    )
    result = rig.run("job", worktree, runtime)
    assert result.outcome == "ok"
    assert result.stdout == "adapter report\n"
    recorded = capture.read_text(encoding="utf-8")
    assert f"--worktree|{worktree}" in recorded
    assert "http://127.0.0.1:1/mcp\ntoken\nf0\n" in recorded


def test_worker_picodex_loads_generated_extension(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    prompt = tmp_path / "prompt"
    prompt.write_text("hello", encoding="utf-8")
    log_dir = tmp_path / "logs"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    args_file = tmp_path / "pi-args"
    executable(
        bin_dir / "pi",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > \"$PI_ARGS\"\ncat\n",
    )
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("PI_ARGS", str(args_file))
    shim = tmp_path / "extension.ts"
    shim.write_text("extension", encoding="utf-8")
    monkeypatch.setenv("WILDFLOWS_PI_EXTENSION", str(shim))
    adapter = Path("rigs/worker-picodex.sh").resolve()
    process = subprocess.run(
        [
            str(adapter),
            "--worktree", str(worktree),
            "--prompt", str(prompt),
            "--log-dir", str(log_dir),
            "--handle-out", str(tmp_path / "handle"),
            "--timeout", "10",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert process.stdout == "hello"
    assert f"-e {shim}" in args_file.read_text(encoding="utf-8")


def test_rig_yaml_builds_frame_rigs_relative_to_config(tmp_path: Path) -> None:
    executable(tmp_path / "adapter", "#!/bin/sh\ncat \"$4\"\n")
    config = tmp_path / "rigs.yaml"
    config.write_text(
        """rigs:
  root:
    kind: script
    script: adapter
    log_dir: logs
    timeout_s: 10
  local:
    kind: shell
    template: "printf done"
    timeout_s: 5
""",
        encoding="utf-8",
    )
    registry = load_rigs(config)
    assert registry.names == frozenset({"root", "local"})
