"""Repository rig adapters exercised through ScriptRig with fake transports."""
from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

from wildflows.rig import Result, ScriptRig

ROOT = Path(__file__).resolve().parents[1]
ADAPTERS = ROOT / "rigs"


def _executable(path: Path, body: str) -> Path:
    path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _stubs(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _executable(bin_dir / "pi", r'''
prompt="$(cat)"
printf '%s' "$prompt" > "${STUB_CAPTURE:?}"
case "${STUB_MODE:?}" in
  planner)
    printf 'model preface\n```json\n{"end":true,"expression":null}\n```\ntrailing noise\n' ;;
  worker) printf 'senior report text' ;;
  busy) echo '429: usage limit reached' >&2; exit 9 ;;
  garbage) printf '```json\nnot-json\n```\n' ;;
  *) exit 2 ;;
esac
''')
    _executable(bin_dir / "curl", r'''
request=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-binary) request="${2#@}"; shift 2 ;;
    -H|--max-time) shift 2 ;;
    --silent|--show-error|--fail-with-body) shift ;;
    http*) shift ;;
    *) echo "fake curl: unknown $1" >&2; exit 2 ;;
  esac
done
python3 - "$request" "${STUB_CAPTURE:?}" <<'PY'
import json, sys
request = json.load(open(sys.argv[1], encoding="utf-8"))
open(sys.argv[2], "w", encoding="utf-8").write(request["messages"][0]["content"])
PY
case "${STUB_MODE:?}" in
  local) printf '{"choices":[{"message":{"content":"local report text"}}]}' ;;
  busy) printf '{"error":"429 rate limit"}'; exit 22 ;;
  garbage) printf 'not-json' ;;
  transport) echo 'connection refused' >&2; exit 7 ;;
  *) exit 2 ;;
esac
''')
    return bin_dir


def _run(
    tmp_path: Path, adapter: str, mode: str, *, prompt: str = "input prompt"
) -> tuple[Result, Path]:
    bin_dir = _stubs(tmp_path)
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    capture = tmp_path / "capture"
    rig = ScriptRig(
        ADAPTERS / adapter,
        tmp_path / "logs",
        timeout_s=10,
        env={
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "STUB_MODE": mode,
            "STUB_CAPTURE": str(capture),
        },
    )
    return rig.run(prompt, worktree), capture


def test_adapters_are_executable_and_bash_valid() -> None:
    for name in ("planner-picodex.sh", "worker-local.sh", "worker-picodex.sh"):
        path = ADAPTERS / name
        assert os.access(path, os.X_OK)
        subprocess.run(["bash", "-n", str(path)], check=True)


def test_planner_adapter_reads_stdin_and_prints_only_valid_json(tmp_path: Path) -> None:
    result, capture = _run(tmp_path, "planner-picodex.sh", "planner")
    assert result.ok
    assert json.loads(result.text) == {"end": True, "expression": None}
    assert result.text.strip() == '{"end":true,"expression":null}'
    assert capture.read_text(encoding="utf-8") == "input prompt"


def test_planner_adapter_rejects_garbage_nonzero(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, "planner-picodex.sh", "garbage")
    assert result.outcome == "failed"
    assert result.exit_code != 0
    assert "malformed decision JSON" in result.text


def test_picodex_worker_returns_text_and_classifies_busy(tmp_path: Path) -> None:
    result, capture = _run(tmp_path, "worker-picodex.sh", "worker")
    assert result.text == "senior report text"
    assert capture.read_text(encoding="utf-8") == "input prompt"

    busy, _ = _run(tmp_path / "busy", "worker-picodex.sh", "busy")
    assert busy.outcome == "busy"
    assert busy.exit_code != 0


def test_local_worker_returns_text_and_surfaces_transport_states(tmp_path: Path) -> None:
    result, capture = _run(tmp_path, "worker-local.sh", "local")
    assert result.text == "local report text"
    assert capture.read_text(encoding="utf-8") == "input prompt"

    busy, _ = _run(tmp_path / "busy", "worker-local.sh", "busy")
    assert busy.outcome == "busy"
    transport, _ = _run(tmp_path / "transport", "worker-local.sh", "transport")
    assert transport.outcome == "failed"
    assert "connection refused" in transport.text
    garbage, _ = _run(tmp_path / "garbage", "worker-local.sh", "garbage")
    assert garbage.outcome == "failed"
    assert garbage.exit_code != 0
