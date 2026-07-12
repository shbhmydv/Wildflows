"""The multi-harness seam: Rig.run(prompt, workdir) -> Result.

prompt in, text/files out — grindstone's shape-agnostic request.sh contract, so real
rigs (claude -p, pi, local Qwen, codex exec) plug in later behind ShellRig with no
engine change. Real rigs are NOT wired up in this build (no network, no model calls).
"""
from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, Field


class Result(BaseModel):
    """A primitive's output: a diff, artifact, free text, or a judgment — all one shape."""

    text: str = ""
    files: list[str] = Field(default_factory=list)
    ok: bool = True
    exit_code: int | None = None


@runtime_checkable
class Rig(Protocol):
    """A harness+model that executes a `do`."""

    def run(self, prompt: str, workdir: Path) -> Result: ...


class EchoRig:
    """Deterministic rig: echoes the prompt back. The test substrate."""

    def run(self, prompt: str, workdir: Path) -> Result:
        return Result(text=f"echo: {prompt}", ok=True, exit_code=0)


class ShellRig:
    """Shells out to a command template with {prompt} substituted; stdout is the result.

    This is the real plug-in path (e.g. template='claude -p {prompt}'); the command
    runs with cwd=workdir so a real rig writes its files inside the worktree.
    """

    def __init__(self, template: str) -> None:
        self.template = template

    def run(self, prompt: str, workdir: Path) -> Result:
        cmd = self.template.replace("{prompt}", shlex.quote(prompt))
        proc = subprocess.run(
            cmd,
            shell=True,
            cwd=workdir,
            capture_output=True,
            text=True,
        )
        return Result(
            text=proc.stdout if proc.returncode == 0 else proc.stderr,
            ok=proc.returncode == 0,
            exit_code=proc.returncode,
        )


class RigRegistry:
    """Resolves a RigRef name to a Rig at execution time."""

    def __init__(self, rigs: dict[str, Rig]) -> None:
        self._rigs = dict(rigs)

    def resolve(self, name: str) -> Rig:
        if name not in self._rigs:
            raise KeyError(f"unknown rig: {name!r}")
        return self._rigs[name]
