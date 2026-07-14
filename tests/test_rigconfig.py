"""YAML rig config: owner-facing rigs.yaml -> a validated RigRegistry."""
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from wildflows.rig import EchoRig, ScriptRig, ShellRig
from wildflows.rigconfig import load_rigs

_GOOD = """\
rigs:
  fast:
    kind: echo
  local:
    kind: shell
    template: "printf '%s' {prompt}"
    timeout_s: 60
  senior:
    kind: script
    script: /path/to/senior_request.sh
    timeout_s: 1800
    log_dir: /tmp/wildflows-logs
    env:
      GRINDSTONE_SENIOR_MODEL: gpt-5.6-sol
    busy_patterns:
      - "429"
      - "usage limit"
"""


def test_load_rigs_round_trip(tmp_path: Path) -> None:
    cfg = tmp_path / "rigs.yaml"
    cfg.write_text(_GOOD, encoding="utf-8")
    registry = load_rigs(cfg)

    assert isinstance(registry.resolve("fast"), EchoRig)
    assert isinstance(registry.resolve("local"), ShellRig)
    senior = registry.resolve("senior")
    assert isinstance(senior, ScriptRig)
    assert senior.timeout_s == 1800
    assert senior.env["GRINDSTONE_SENIOR_MODEL"] == "gpt-5.6-sol"
    assert senior.script == Path("/path/to/senior_request.sh")

    # The echo rig actually works after resolution.
    assert registry.resolve("fast").run("hi", tmp_path).ok is True


def test_relative_script_paths_resolve_from_rigs_file(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    cfg = config_dir / "rigs.yaml"
    cfg.write_text(
        "rigs:\n  worker:\n    kind: script\n    script: ../worker.sh\n"
        "    log_dir: logs\n",
        encoding="utf-8",
    )
    worker = load_rigs(cfg).resolve("worker")
    assert isinstance(worker, ScriptRig)
    assert worker.script == tmp_path / "worker.sh"
    assert worker.log_dir == config_dir / "logs"


def test_load_rigs_rejects_unknown_kind(tmp_path: Path) -> None:
    cfg = tmp_path / "rigs.yaml"
    cfg.write_text("rigs:\n  weird:\n    kind: telepathy\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        load_rigs(cfg)


def test_load_rigs_rejects_missing_script_field(tmp_path: Path) -> None:
    cfg = tmp_path / "rigs.yaml"
    cfg.write_text("rigs:\n  s:\n    kind: script\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        load_rigs(cfg)


def test_load_rigs_rejects_nonpositive_timeout(tmp_path: Path) -> None:
    # A non-positive rig timeout is rejected at config load (item 5, Field(gt=0)).
    cfg = tmp_path / "rigs.yaml"
    cfg.write_text(
        "rigs:\n  s:\n    kind: shell\n    template: 'echo {prompt}'\n    timeout_s: 0\n",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError):
        load_rigs(cfg)


def test_example_rigs_yaml_loads() -> None:
    root = Path(__file__).resolve().parents[1]
    registry = load_rigs(root / "examples" / "rigs.yaml")
    assert isinstance(registry.resolve("echo"), EchoRig)
    assert isinstance(registry.resolve("shell-claude"), ShellRig)

    toy = load_rigs(root / "examples" / "toy-run" / "rigs.yaml")
    for name, script in (
        ("planner", "planner-picodex.sh"),
        ("local", "worker-local.sh"),
        ("senior", "worker-picodex.sh"),
    ):
        adapter = toy.resolve(name)
        assert isinstance(adapter, ScriptRig)
        assert adapter.script == root / "rigs" / script
        assert adapter.script.is_file()
