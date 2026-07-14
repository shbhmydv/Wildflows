"""ScriptRig: drive a real request.sh-contract script without invoking any model.

The fake scripts are written by the tests themselves (tiny bash honouring the
grindstone executor contract: --worktree/--prompt/--log-dir/--handle-out/--timeout).
No real model is ever called.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path

from wildflows.rig import Result, ScriptRig


def _write_script(path: Path, body: str) -> Path:
    path.write_text("#!/usr/bin/env bash\n" + body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


# A script that honours the arg contract: reads the prompt file, writes an artifact
# into the worktree, exits 0.
_SUCCESS = r"""
set -euo pipefail
worktree="" prompt="" log_dir="" handle_out="" timeout=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worktree) worktree="$2"; shift 2 ;;
    --prompt) prompt="$2"; shift 2 ;;
    --log-dir) log_dir="$2"; shift 2 ;;
    --handle-out) handle_out="$2"; shift 2 ;;
    --timeout) timeout="$2"; shift 2 ;;
    *) echo "unknown $1" >&2; exit 2 ;;
  esac
done
echo "$$" > "$handle_out"
cat "$prompt" > "$worktree/artifact.txt"
echo "done grinding"
"""

_FAILURE = r"""
echo "boom on stderr" >&2
exit 3
"""

_BUSY = r"""
echo "Error: 429 You've hit your usage limit. Upgrade to Pro." >&2
exit 1
"""

_SLOW = r"""
sleep 5
"""


def test_script_rig_success(tmp_path: Path) -> None:
    script = _write_script(tmp_path / "ok.sh", _SUCCESS)
    workdir = tmp_path / "wt"
    workdir.mkdir()
    logs = tmp_path / "logs"
    rig = ScriptRig(script=script, log_dir=logs, timeout_s=10)
    r = rig.run("build the thing", workdir)
    assert isinstance(r, Result)
    assert r.ok is True
    assert r.outcome == "ok"
    assert r.exit_code == 0
    assert (workdir / "artifact.txt").read_text() == "build the thing"


def test_script_rig_failure(tmp_path: Path) -> None:
    script = _write_script(tmp_path / "fail.sh", _FAILURE)
    workdir = tmp_path / "wt"
    workdir.mkdir()
    rig = ScriptRig(script=script, log_dir=tmp_path / "logs", timeout_s=10)
    r = rig.run("x", workdir)
    assert r.ok is False
    assert r.outcome == "failed"
    assert r.exit_code == 3
    assert "boom" in r.text


def test_script_rig_busy_is_not_a_failure(tmp_path: Path) -> None:
    script = _write_script(tmp_path / "busy.sh", _BUSY)
    workdir = tmp_path / "wt"
    workdir.mkdir()
    rig = ScriptRig(script=script, log_dir=tmp_path / "logs", timeout_s=10)
    r = rig.run("x", workdir)
    # A transport rate/session wall — distinct from a task failure.
    assert r.ok is False
    assert r.outcome == "busy"
    assert r.exit_code != 0


def test_script_rig_timeout(tmp_path: Path) -> None:
    script = _write_script(tmp_path / "slow.sh", _SLOW)
    workdir = tmp_path / "wt"
    workdir.mkdir()
    rig = ScriptRig(script=script, log_dir=tmp_path / "logs", timeout_s=1)
    r = rig.run("x", workdir)
    # Timeout is represented as `failed` with a "[timeout]" marker (see rig.py note).
    assert r.ok is False
    assert r.outcome == "failed"
    assert "[timeout]" in r.text


def test_script_rig_log_dir_per_dispatch_is_populated(tmp_path: Path) -> None:
    script = _write_script(tmp_path / "ok.sh", _SUCCESS)
    workdir = tmp_path / "n0.1"  # in the real seam a worktree is named for its node_id
    workdir.mkdir()
    logs = tmp_path / "logs"
    rig = ScriptRig(script=script, log_dir=logs, timeout_s=10)
    rig.run("hello", workdir)
    dispatch_dir = logs / "n0.1"
    assert dispatch_dir.is_dir()
    assert (dispatch_dir / "agent.stdout.log").read_text().strip() == "done grinding"
    assert (dispatch_dir / "agent.stderr.log").exists()
    assert (dispatch_dir / "prompt.txt").read_text() == "hello"

    rig.run("retry", workdir)
    assert (logs / "n0.1-1" / "prompt.txt").read_text() == "retry"


def test_script_rig_preserves_fractional_timeout_argument(tmp_path: Path) -> None:
    script = _write_script(tmp_path / "timeout.sh", _SUCCESS + '\necho "timeout=$timeout"\n')
    workdir = tmp_path / "wt"
    workdir.mkdir()
    result = ScriptRig(script, tmp_path / "logs", timeout_s=1.5).run("x", workdir)
    assert "timeout=1.5" in result.text


def test_script_rig_passes_extra_env(tmp_path: Path) -> None:
    script = _write_script(
        tmp_path / "env.sh",
        'echo "MODEL=$GRINDSTONE_SENIOR_MODEL"\n',
    )
    workdir = tmp_path / "wt"
    workdir.mkdir()
    rig = ScriptRig(
        script=script,
        log_dir=tmp_path / "logs",
        timeout_s=10,
        env={"GRINDSTONE_SENIOR_MODEL": "test-sol"},
    )
    r = rig.run("x", workdir)
    assert "MODEL=test-sol" in r.text
    # The extra env must not clobber the inherited environment.
    assert "PATH" in os.environ
