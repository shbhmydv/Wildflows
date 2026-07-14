"""The argparse entry point can park and resume an Ask with --answer."""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

from tests.test_engine import init_repo
from wildflows.journal import Journal

ROOT = Path(__file__).resolve().parents[1]


def test_cli_run_then_resume_with_answer(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    job = tmp_path / "job.md"
    job.write_text("Ask once, then finish.", encoding="utf-8")
    planner = tmp_path / "planner.sh"
    planner.write_text(
        r'''#!/usr/bin/env bash
set -euo pipefail
prompt=""; handle=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prompt) prompt="$2"; shift 2 ;;
    --handle-out) handle="$2"; shift 2 ;;
    --worktree|--log-dir|--timeout) shift 2 ;;
    *) exit 2 ;;
  esac
done
echo "$$" > "$handle"
cat "$prompt" > /dev/null
count_file="${CLI_CALLS:?}"
count=$(cat "$count_file" 2>/dev/null || echo 0)
count=$((count + 1)); echo "$count" > "$count_file"
if [[ "$count" -eq 1 ]]; then
  echo '{"expression":{"kind":"ask","question":"Proceed?"},"rails":{"deadline_s":60,"max_epochs":3},"rationale":"ask","end":false,"final_summary":null}'
else
  echo '{"expression":null,"rails":{"deadline_s":60,"max_epochs":3},"rationale":"done","end":true,"final_summary":"answered"}'
fi
''',
        encoding="utf-8",
    )
    planner.chmod(planner.stat().st_mode | stat.S_IXUSR)
    config = tmp_path / "rigs.yaml"
    config.write_text(
        "rigs:\n  planner:\n    kind: script\n    script: planner.sh\n"
        "    log_dir: logs\n    timeout_s: 10\n",
        encoding="utf-8",
    )
    env = {**os.environ, "CLI_CALLS": str(tmp_path / "calls")}
    common = [
        str(job), "--repo", str(repo), "--rigs", str(config), "--run-id", "cli-run",
    ]

    parked = subprocess.run(
        [sys.executable, "-m", "wildflows", "run", *common],
        cwd=ROOT, env=env, capture_output=True, text=True,
    )
    assert parked.returncode != 0
    assert "AwaitingOwner" in parked.stderr

    resumed = subprocess.run(
        [sys.executable, "-m", "wildflows", "resume", *common, "--answer", "yes"],
        cwd=ROOT, env=env, capture_output=True, text=True,
    )
    assert resumed.returncode == 0, resumed.stderr
    assert json.loads(resumed.stdout.splitlines()[-1]) == {
        "summary": "answered", "epochs": 1,
    }
    state = Journal.load(repo / ".wildflows" / "runs" / "cli-run").projection
    assert state.results[(0, "n0")].text == "yes"
