"""Pass-13 regression: core Git commands share the durable process barrier."""
from __future__ import annotations

import ast
import json
import os
import shlex
import stat
import tempfile
import time
from pathlib import Path

import pytest

from wildflows.engine import Engine, replay
from wildflows.expr import Do, RigRef
from wildflows.rig import EchoRig, RigRegistry, ShellRig

from tests.test_review_fixes_pass12 import (
    _fork_engine,
    _kill_and_wait,
    _process_identity,
    _same_process_is_live,
    _wait_for,
)
from tests.test_review_fixes_pass5 import _base_repo


def test_engine_process_launch_sites_are_all_explicitly_supervised() -> None:
    sites: list[tuple[str, str]] = []

    class LaunchVisitor(ast.NodeVisitor):
        def __init__(self, module: str) -> None:
            self.scope = [module]
            self.aliases: dict[str, str] = {}

        def visit_Import(self, node: ast.Import) -> None:
            for alias in node.names:
                self.aliases[alias.asname or alias.name] = alias.name

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
            if node.module is not None:
                for alias in node.names:
                    self.aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.scope.append(node.name); self.generic_visit(node); self.scope.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self.scope.append(node.name); self.generic_visit(node); self.scope.pop()

        def visit_Call(self, node: ast.Call) -> None:
            raw = ast.unparse(node.func)
            root, dot, rest = raw.partition(".")
            name = self.aliases.get(root, root) + (dot + rest if dot else "")
            exact = {
                "subprocess.run", "subprocess.Popen", "subprocess.call",
                "subprocess.check_call", "subprocess.check_output", "os.fork",
                "os.system", "os.popen", "multiprocessing.Process",
                "asyncio.create_subprocess_exec", "asyncio.create_subprocess_shell",
            }
            if name in exact or name.startswith(("os.exec", "os.spawn", "os.posix_spawn")):
                sites.append((".".join(self.scope), name))
            self.generic_visit(node)

    for path in sorted(Path("wildflows").rglob("*.py")):
        LaunchVisitor(path.stem).visit(ast.parse(path.read_text(encoding="utf-8")))
    assert sites == [
        ("rig.ShellRig.run.execute", "subprocess.run"),
        ("rig.ScriptRig.run.execute", "subprocess.run"),
        ("workspace.WorkspaceEffects._core_scope_child", "subprocess.run"),
        ("workspace.WorkspaceEffects.core_process_scope", "os.fork"),
        ("workspace.WorkspaceEffects.run_process", "os.fork"),
        ("workspace.WorkspaceEffects.run_predicate.execute", "subprocess.run"),
    ]


def test_core_git_scope_transports_non_utf8_temporary_index_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    raw_temp = os.path.join(os.fsencode(tmp_path), b"tmp-\xff")
    os.mkdir(raw_temp)
    monkeypatch.setattr(tempfile, "tempdir", os.fsdecode(raw_temp))
    run_dir = tmp_path / "run"

    Engine(run_dir, workdir, RigRegistry({"echo": EchoRig()})).run_epoch(
        Do(task="no effect", rig=RigRef(name="echo")), 0
    )
    assert replay(run_dir).epoch_closed(0)


def test_engine_crash_mid_core_git_hook_reaps_same_group_before_recovery_and_prevents_post_close_write(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    hook_started = tmp_path / "hook-started"
    delayed_started = tmp_path / "delayed-writer-started"
    delayed_pid = tmp_path / "delayed-writer-pid"
    release_hook = tmp_path / "release-hook"

    hook = workdir / ".git" / "hooks" / "pre-commit"
    hook.write_text(
        "#!/bin/sh\n"
        f"if test ! -e {shlex.quote(str(hook_started))}; then\n"
        f"  : > {shlex.quote(str(hook_started))}\n"
        f"  (: > {shlex.quote(str(delayed_started))}; sleep 1.5; "
        f"printf ORPHAN > {shlex.quote(str(workdir / 'base.txt'))}) &\n"
        f"  echo $! > {shlex.quote(str(delayed_pid))}\n"
        f"  while test ! -e {shlex.quote(str(release_hook))}; do sleep 0.01; done\n"
        "fi\n",
        encoding="utf-8",
    )
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR)
    tree = Do(task="write an effect", rig=RigRef(name="shell"))
    registry = RigRegistry({
        "shell": ShellRig("printf effect > effect.txt", timeout_s=30),
    })

    engine_pid = _fork_engine(run_dir, workdir, tree, registry)
    try:
        _wait_for(hook_started, "core git commit did not enter the pre-commit hook")
        _wait_for(delayed_started, "pre-commit hook did not start its delayed writer")
        _wait_for(delayed_pid, "pre-commit hook did not publish the delayed writer pid")
        child_pid = int(delayed_pid.read_text(encoding="ascii").strip())
        identity = _process_identity(child_pid)
        assert identity is not None
        child_start = identity[0]
        core_records = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (run_dir / "processes").glob("*.json")
            if json.loads(path.read_text(encoding="utf-8"))["kind"] == "core"
        ]
        assert len(core_records) == 1
        assert os.getpgid(child_pid) == core_records[0]["pgid"]
        _kill_and_wait(engine_pid)
        assert _same_process_is_live(child_pid, child_start)

        # Construction reaps the prior group; run_epoch has not started recovery yet.
        resumed = Engine(run_dir, workdir, registry)
        assert not _same_process_is_live(child_pid, child_start)
        resumed.run_epoch(tree, 0)
        assert replay(run_dir).epoch_closed(0)
        assert (workdir / "base.txt").read_bytes() == b"base"
        time.sleep(1.7)
        assert (workdir / "base.txt").read_bytes() == b"base"
        assert not list((run_dir / "processes").glob("*.json"))
    finally:
        release_hook.touch()
        _kill_and_wait(engine_pid)


def test_core_git_normal_completion_reaps_hook_writer_before_it_can_mutate(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    hook = workdir / ".git" / "hooks" / "pre-commit"
    hook.write_text(
        "#!/bin/sh\n"
        f"(sleep 0.3; printf LATE > {shlex.quote(str(workdir / 'base.txt'))}) &\n",
        encoding="utf-8",
    )
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR)
    tree = Do(task="write an effect", rig=RigRef(name="shell"))
    registry = RigRegistry({
        "shell": ShellRig("printf effect > effect.txt", timeout_s=30),
    })

    Engine(run_dir, workdir, registry).run_epoch(tree, 0)
    assert replay(run_dir).epoch_closed(0)
    assert (workdir / "base.txt").read_bytes() == b"base"
    time.sleep(0.4)
    assert (workdir / "base.txt").read_bytes() == b"base"
    assert not list((run_dir / "processes").glob("*.json"))
