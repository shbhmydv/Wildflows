"""The multi-harness seam: ``Rig.run(prompt, workdir) -> Result``.

External commands use a small in-process process-group barrier.  Timeout kills the
whole group; there are no durable process records or restart reaper because every
attempt path is disposable and never reused.
"""
from __future__ import annotations

import os
import re
import shlex
import signal
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TextIO, runtime_checkable

from wildflows.result import Outcome, Result

__all__ = [
    "Outcome",
    "Result",
    "DEFAULT_BUSY_PATTERNS",
    "ExternalResult",
    "run_shell",
    "Rig",
    "EchoRig",
    "ShellRig",
    "ScriptRig",
    "RigRegistry",
]

DEFAULT_BUSY_PATTERNS = [
    r"rate.?limit",
    r"429",
    r"quota",
    r"usage limit",
    r"session limit",
    r"weekly limit",
    r"plan limit",
    r"too many requests",
]


@dataclass(frozen=True)
class ExternalResult:
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool = False


def _kill_group(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _capture(
    command: str | list[str],
    *,
    cwd: Path,
    timeout_s: float,
    shell: bool,
    env: dict[str, str] | None = None,
) -> ExternalResult:
    # Real files cannot be held pipe-open by a background descendant.
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stdout, \
            tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr:
        out_file: TextIO = stdout
        err_file: TextIO = stderr
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=out_file,
            stderr=err_file,
            text=True,
            shell=shell,
            env=env,
            start_new_session=True,
        )
        timed_out = False
        try:
            returncode: int | None = process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            returncode = None
        finally:
            # Also quiesce ordinary background children before the core inspects commits.
            # A deliberate setsid escape remains possible but can only write the abandoned
            # attempt path after this call returns.
            _kill_group(process)
        stdout.seek(0)
        stderr.seek(0)
        return ExternalResult(returncode, stdout.read(), stderr.read(), timed_out)


def run_shell(command: str, workdir: Path, timeout_s: float) -> ExternalResult:
    return _capture(command, cwd=workdir, timeout_s=timeout_s, shell=True)


@runtime_checkable
class Rig(Protocol):
    def run(self, prompt: str, workdir: Path) -> Result: ...


class EchoRig:
    def run(self, prompt: str, workdir: Path) -> Result:
        return Result(text=f"echo: {prompt}", exit_code=0)


class ShellRig:
    def __init__(self, template: str, timeout_s: float) -> None:
        self.template = template
        self.timeout_s = timeout_s

    def run(self, prompt: str, workdir: Path) -> Result:
        command = self.template.replace("{prompt}", shlex.quote(prompt))
        result = run_shell(command, workdir, self.timeout_s)
        if result.timed_out:
            return Result(
                text=f"[timeout] command exceeded {self.timeout_s}s\n{result.stderr}",
                outcome="failed",
            )
        assert result.returncode is not None
        ok = result.returncode == 0
        return Result(
            text=result.stdout if ok else result.stderr,
            exit_code=result.returncode,
            outcome="ok" if ok else "failed",
        )


class ScriptRig:
    """Drive the existing grindstone-compatible executor script contract."""

    def __init__(
        self,
        script: Path,
        log_dir: Path,
        timeout_s: float = 900.0,
        env: dict[str, str] | None = None,
        busy_patterns: list[str] | None = None,
    ) -> None:
        self.script = Path(script)
        self.log_dir = Path(log_dir)
        self.timeout_s = timeout_s
        self.env = dict(env or {})
        self._busy_re = re.compile(
            "|".join(busy_patterns or DEFAULT_BUSY_PATTERNS), re.IGNORECASE
        )

    def _classify(self, returncode: int, stdout: str, stderr: str) -> Outcome:
        if returncode == 0:
            return "ok"
        if self._busy_re.search(stderr) or self._busy_re.search(stdout):
            return "busy"
        return "failed"

    def run(self, prompt: str, workdir: Path) -> Result:
        dispatch_dir = self.log_dir / workdir.name
        dispatch_dir.mkdir(parents=True, exist_ok=True)
        prompt_file = dispatch_dir / "prompt.txt"
        prompt_file.write_text(prompt, encoding="utf-8")
        handle_out = dispatch_dir / "handle"
        argv = [
            str(self.script),
            "--worktree",
            str(workdir),
            "--prompt",
            str(prompt_file),
            "--log-dir",
            str(dispatch_dir),
            "--handle-out",
            str(handle_out),
            "--timeout",
            str(int(self.timeout_s)),
        ]
        result = _capture(
            argv,
            cwd=workdir,
            timeout_s=self.timeout_s,
            shell=False,
            env={**os.environ, **self.env},
        )
        (dispatch_dir / "agent.stdout.log").write_text(result.stdout, encoding="utf-8")
        (dispatch_dir / "agent.stderr.log").write_text(result.stderr, encoding="utf-8")
        if result.timed_out:
            return Result(
                text=f"[timeout] script exceeded {self.timeout_s}s\n{result.stderr}",
                outcome="failed",
            )
        assert result.returncode is not None
        outcome = self._classify(result.returncode, result.stdout, result.stderr)
        text = result.stdout if result.returncode == 0 else (result.stderr or result.stdout)
        return Result(text=text, exit_code=result.returncode, outcome=outcome)


class RigRegistry:
    def __init__(self, rigs: dict[str, Rig]) -> None:
        self._rigs = dict(rigs)

    def __contains__(self, name: object) -> bool:
        return name in self._rigs

    def resolve(self, name: str) -> Rig:
        if name not in self._rigs:
            raise KeyError(f"unknown rig: {name!r}")
        return self._rigs[name]
