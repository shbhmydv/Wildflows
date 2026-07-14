"""Combine executes its inputs, then an ordinary Do with durable result context."""
from __future__ import annotations

import stat
from pathlib import Path

import pytest

from tests.test_engine import init_repo
from wildflows.engine import CombineDependencyError, Engine
from wildflows.expr import Combine, Dispatch, Do, RigRef
from wildflows.rig import RigRegistry, ScriptRig


def _adapter(path: Path) -> Path:
    path.write_text(
        r'''#!/usr/bin/env bash
set -euo pipefail
worktree=""; prompt=""; handle_out=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worktree) worktree="$2"; shift 2 ;;
    --prompt) prompt="$2"; shift 2 ;;
    --log-dir|--timeout) shift 2 ;;
    --handle-out) handle_out="$2"; shift 2 ;;
    *) exit 2 ;;
  esac
done
echo "$$" > "$handle_out"
body="$(cat "$prompt")"
if [[ "$body" == "swarm-0" || "$body" == "swarm-1" ]]; then
  printf '%s\n' "$body" >> "${CALLS:?}"
  if [[ "$body" == "swarm-1" && "${FAIL:-}" == "always" ]]; then
    echo "upstream failed" >&2; exit 7
  fi
  if [[ "$body" == "swarm-1" && "${FAIL:-}" == "once" && ! -e "${MARKER:?}" ]]; then
    touch "$MARKER"; echo "upstream failed once" >&2; exit 7
  fi
  [[ "$body" == "swarm-0" ]] && printf alpha || printf beta
else
  printf 'combine\n' >> "${CALLS:?}"
  cp "$prompt" "$worktree/combine-prompt.txt"
  printf combined
fi
''',
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _tree() -> Combine:
    return Combine(
        task="Synthesize the swarm in declared order.",
        rig=RigRef(name="script"),
        inputs=[Dispatch(children=[
            Do(task="swarm-0", rig=RigRef(name="script")),
            Do(task="swarm-1", rig=RigRef(name="script")),
        ])],
    )


def _engine(tmp_path: Path, *, failure: str = "") -> tuple[Engine, Path]:
    repo = tmp_path / "repo"
    init_repo(repo)
    calls = tmp_path / "calls"
    rig = ScriptRig(
        _adapter(tmp_path / "adapter.sh"),
        tmp_path / "logs",
        timeout_s=10,
        env={"CALLS": str(calls), "FAIL": failure, "MARKER": str(tmp_path / "marker")},
    )
    return Engine(tmp_path / "run", repo, RigRegistry({"script": rig}), max_workers=2), calls


def test_swarm_combine_injects_all_results_and_artifact_links(tmp_path: Path) -> None:
    engine, calls = _engine(tmp_path)
    engine.run_epoch(_tree(), 0)

    prompt = (engine.workdir / "combine-prompt.txt").read_text(encoding="utf-8")
    assert prompt.startswith("Synthesize the swarm in declared order.")
    assert prompt.index('"node_id": "n0.0.0"') < prompt.index('"node_id": "n0.0.1"')
    assert '"text": "alpha"' in prompt and '"text": "beta"' in prompt
    assert '"artifact": "artifacts/e0-n0.0.0/result-' in prompt
    assert f'"artifact_path": "{engine.run_dir}/artifacts/' in prompt
    assert calls.read_text(encoding="utf-8").splitlines().count("combine") == 1
    assert engine.journal.projection.results[(0, "n0")].text == "combined"
    assert engine.journal.projection.epoch_closed(0)


def test_failed_combine_input_is_typed_and_combiner_does_not_start(tmp_path: Path) -> None:
    engine, calls = _engine(tmp_path, failure="always")
    with pytest.raises(CombineDependencyError, match="combine input failed"):
        engine.run_epoch(_tree(), 0)

    assert "combine" not in calls.read_text(encoding="utf-8").splitlines()
    assert not engine.journal.projection.epoch_closed(0)


def test_resume_mid_swarm_reuses_success_before_combine(tmp_path: Path) -> None:
    engine, calls = _engine(tmp_path, failure="once")
    with pytest.raises(CombineDependencyError):
        engine.run_epoch(_tree(), 0)
    assert "combine" not in calls.read_text(encoding="utf-8").splitlines()

    resumed = Engine(engine.run_dir, engine.workdir, engine.registry, max_workers=2)
    resumed.run_epoch(_tree(), 0)

    invoked = calls.read_text(encoding="utf-8").splitlines()
    assert invoked.count("swarm-0") == 1
    assert invoked.count("swarm-1") == 2
    assert invoked.count("combine") == 1
    assert resumed.journal.projection.epoch_closed(0)
