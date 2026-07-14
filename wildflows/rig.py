"""Frame rig adapters: prompt + CWD + engine capability -> resident agent result."""
from __future__ import annotations

import ctypes
import os
import re
import shlex
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, TextIO, runtime_checkable

from wildflows.frame import FrameOutcome, FrameResult, FrameRuntime

__all__ = [
    "DEFAULT_BUSY_PATTERNS",
    "ExternalResult",
    "run_shell",
    "Rig",
    "EchoRig",
    "ShellRig",
    "ScriptRig",
    "RigRegistry",
]

_PRCTL: Callable[[int, int, int, int, int], int] | None = None
if os.name == "posix" and Path("/proc").is_dir():
    _libc = ctypes.CDLL(None, use_errno=True)
    _raw_prctl = _libc.prctl
    _raw_prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    _raw_prctl.restype = ctypes.c_int
    _PRCTL = _raw_prctl


def _die_with_parent(parent_pid: int) -> None:
    if _PRCTL is None or _PRCTL(1, signal.SIGKILL, 0, 0, 0) != 0:
        os._exit(127)
    if os.getppid() != parent_pid:
        os._exit(127)


def _guarded_child(parent_pid: int) -> None:
    """Fence the workload and kill its whole process group after parent death."""
    _die_with_parent(parent_pid)
    leader_pid = os.getpid()
    watchdog_pid = os.fork()
    if watchdog_pid != 0:
        return
    while os.getppid() == leader_pid:
        time.sleep(0.02)
    try:
        os.killpg(leader_pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    os._exit(0)


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
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stdout, \
            tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr:
        out_file: TextIO = stdout
        err_file: TextIO = stderr
        parent_pid = os.getpid()
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=out_file,
            stderr=err_file,
            text=True,
            shell=shell,
            env=env,
            start_new_session=True,
            preexec_fn=(lambda: _guarded_child(parent_pid)) if _PRCTL else None,
        )
        timed_out = False
        try:
            returncode: int | None = process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            returncode = None
        finally:
            _kill_group(process)
        stdout.seek(0)
        stderr.seek(0)
        return ExternalResult(returncode, stdout.read(), stderr.read(), timed_out)


def run_shell(
    command: str,
    workdir: Path,
    timeout_s: float,
    env: dict[str, str] | None = None,
) -> ExternalResult:
    return _capture(command, cwd=workdir, timeout_s=timeout_s, shell=True, env=env)


def _runtime_env(runtime: FrameRuntime) -> dict[str, str]:
    return {
        "WILDFLOWS_MCP_URL": runtime.endpoint,
        "WILDFLOWS_RUN_TOKEN": runtime.token,
        "WILDFLOWS_FRAME_ID": runtime.frame_id,
        "WILDFLOWS_PI_EXTENSION": str(runtime.shim_path),
        "WILDFLOWS_NEXT_CALL_INDEX": str(runtime.next_call_index),
    }


@runtime_checkable
class Rig(Protocol):
    timeout_s: float

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult: ...


class EchoRig:
    timeout_s = 30.0

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del workdir, runtime
        text = f"echo: {prompt}"
        return FrameResult(text=text, exit_code=0, stdout=text)


class ShellRig:
    def __init__(self, template: str, timeout_s: float) -> None:
        self.template = template
        self.timeout_s = timeout_s

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        command = self.template.replace("{prompt}", shlex.quote(prompt))
        result = run_shell(
            command,
            workdir,
            self.timeout_s,
            env={**os.environ, **_runtime_env(runtime)},
        )
        if result.timed_out:
            return FrameResult(
                text=f"[timeout] command exceeded {self.timeout_s}s\n{result.stderr}",
                outcome="failed",
                stdout=result.stdout,
                stderr=result.stderr,
            )
        assert result.returncode is not None
        outcome: FrameOutcome = "ok" if result.returncode == 0 else "failed"
        text = result.stdout if result.returncode == 0 else (result.stderr or result.stdout)
        return FrameResult(
            text=text,
            exit_code=result.returncode,
            outcome=outcome,
            stdout=result.stdout,
            stderr=result.stderr,
        )


class ScriptRig:
    """Drive the grindstone-compatible script contract as a v2 frame."""

    def __init__(
        self,
        script: Path,
        log_dir: Path,
        timeout_s: float = 900.0,
        env: dict[str, str] | None = None,
        busy_patterns: list[str] | None = None,
    ) -> None:
        self.script = Path(script).resolve()
        self.log_dir = Path(log_dir).resolve()
        self.timeout_s = timeout_s
        self.env = dict(env or {})
        self._busy_re = re.compile(
            "|".join(busy_patterns or DEFAULT_BUSY_PATTERNS), re.IGNORECASE
        )

    def _classify(
        self, returncode: int, stdout: str, stderr: str
    ) -> FrameOutcome:
        if returncode == 0:
            return "ok"
        if self._busy_re.search(stderr) or self._busy_re.search(stdout):
            return "busy"
        return "failed"

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        suffix = 0
        while True:
            name = workdir.name if suffix == 0 else f"{workdir.name}-{suffix}"
            dispatch_dir = self.log_dir / name
            try:
                dispatch_dir.mkdir()
                break
            except FileExistsError:
                suffix += 1
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
            str(self.timeout_s),
        ]
        result = _capture(
            argv,
            cwd=workdir,
            timeout_s=self.timeout_s,
            shell=False,
            env={**os.environ, **self.env, **_runtime_env(runtime)},
        )
        (dispatch_dir / "agent.stdout.log").write_text(result.stdout, encoding="utf-8")
        (dispatch_dir / "agent.stderr.log").write_text(result.stderr, encoding="utf-8")
        if result.timed_out:
            return FrameResult(
                text=f"[timeout] script exceeded {self.timeout_s}s\n{result.stderr}",
                outcome="failed",
                stdout=result.stdout,
                stderr=result.stderr,
            )
        assert result.returncode is not None
        outcome = self._classify(result.returncode, result.stdout, result.stderr)
        text = result.stdout if result.returncode == 0 else (result.stderr or result.stdout)
        return FrameResult(
            text=text,
            exit_code=result.returncode,
            outcome=outcome,
            stdout=result.stdout,
            stderr=result.stderr,
        )


class RigRegistry:
    def __init__(self, rigs: dict[str, Rig]) -> None:
        self._rigs = dict(rigs)

    def __contains__(self, name: object) -> bool:
        return name in self._rigs

    def resolve(self, name: str) -> Rig:
        if name not in self._rigs:
            raise KeyError(f"unknown rig: {name!r}")
        return self._rigs[name]

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._rigs)
