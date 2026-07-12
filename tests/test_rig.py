"""Rig seam: one method run(prompt, workdir) -> Result; two implementations."""
from __future__ import annotations

from pathlib import Path

from wildflows.rig import EchoRig, RigRegistry, Result, ShellRig


def test_echo_rig_is_deterministic(tmp_path: Path) -> None:
    rig = EchoRig()
    r1 = rig.run("hello", tmp_path)
    r2 = rig.run("hello", tmp_path)
    assert isinstance(r1, Result)
    assert r1.ok is True
    assert r1.text == r2.text
    assert "hello" in r1.text


def test_shell_rig_runs_command_template(tmp_path: Path) -> None:
    rig = ShellRig(template="printf '%s' {prompt}")
    r = rig.run("wildflows", tmp_path)
    assert r.ok is True
    assert r.exit_code == 0
    assert r.text.strip() == "wildflows"


def test_shell_rig_nonzero_exit_is_not_ok(tmp_path: Path) -> None:
    rig = ShellRig(template="false")
    r = rig.run("x", tmp_path)
    assert r.ok is False
    assert r.exit_code != 0


def test_registry_resolves_by_name(tmp_path: Path) -> None:
    reg = RigRegistry({"echo": EchoRig()})
    rig = reg.resolve("echo")
    assert rig.run("t", tmp_path).ok is True
