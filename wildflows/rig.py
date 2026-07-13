"""The multi-harness seam: Rig.run(prompt, workdir) -> Result.

prompt in, text/files out — grindstone's shape-agnostic request.sh contract, so real
rigs (claude -p, pi, local Qwen, codex exec) plug in later behind ShellRig with no
engine change. Real rigs are NOT wired up in this build (no network, no model calls).
"""
from __future__ import annotations

import os
import re
import shlex
import signal
import subprocess
from pathlib import Path
from typing import Protocol, runtime_checkable

from wildflows.result import Outcome, Result

# `Result`/`Outcome` are core domain values (in `result.py`); re-exported here so
# `from wildflows.rig import Result` keeps working for rig implementations.
__all__ = [
    "Outcome", "Result", "DEFAULT_BUSY_PATTERNS", "Rig", "EchoRig", "ShellRig",
    "ScriptRig", "RigRegistry",
]

# The rate/session-limit signatures the grindstone rigs surface on stderr (kept in
# sync with models/picodex/{senior,planner}_request.sh + grindstone/ratelimit.py).
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


@runtime_checkable
class Rig(Protocol):
    """A harness+model that executes a `do`."""

    def run(self, prompt: str, workdir: Path) -> Result: ...


class EchoRig:
    """Deterministic rig: echoes the prompt back. The test substrate."""

    def run(self, prompt: str, workdir: Path) -> Result:
        return Result(text=f"echo: {prompt}", exit_code=0)


class ShellRig:
    """Shells out to a command template with {prompt} substituted; stdout is the result.

    This is the real plug-in path (e.g. template='claude -p {prompt}'); the command
    runs with cwd=workdir so a real rig writes its files inside the worktree.

    `timeout_s` is REQUIRED — an unbounded rig can hang an epoch forever. The
    command runs in its OWN process group (`start_new_session=True`); on timeout the
    core signals the whole GROUP (`killpg`), so background children the shell spawned
    (`cmd & ...`) are reaped too, not orphaned. A timeout is
    returned as `outcome="failed"` with a `[timeout]` marker; any non-zero exit is
    likewise `outcome="failed"`.
    """

    def __init__(self, template: str, timeout_s: float) -> None:
        self.template = template
        self.timeout_s = timeout_s

    def run(self, prompt: str, workdir: Path) -> Result:
        cmd = self.template.replace("{prompt}", shlex.quote(prompt))
        proc = subprocess.Popen(
            cmd,
            shell=True,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,  # own process group -> killpg reaps descendants
        )
        try:
            stdout, stderr = proc.communicate(timeout=self.timeout_s)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
            stdout, stderr = proc.communicate()
            return Result(
                text=f"[timeout] command exceeded {self.timeout_s}s\n{stderr}",
                exit_code=None,
                outcome="failed",
            )
        ok = proc.returncode == 0
        return Result(
            text=stdout if ok else stderr,
            exit_code=proc.returncode,
            outcome="ok" if ok else "failed",
        )


class ScriptRig:
    """Drives a real grindstone-contract executor script — the production rig seam.

    The script is invoked with EXACTLY the battle-tested request.sh argument contract:

        <script> --worktree <dir> --prompt <file> --log-dir <dir> \
                 --handle-out <file> --timeout <secs>

    It grinds agentically inside the worktree, propagates its exit code, and surfaces
    rate/session-limit signatures on stderr. `--prompt` is a FILE PATH (not inline
    argv): the real rigs feed the prompt on stdin from that file to dodge the kernel's
    MAX_ARG_STRLEN wall on large prior-failure context — ScriptRig writes the prompt to
    `<dispatch-log>/prompt.txt` and passes its path, mirroring the contract exactly.

    Per-dispatch logs land under `<log_dir>/<workdir.name>/` — in the real worktree
    seam (ladder step 4) each `do` runs in a worktree named for its node_id, so the
    dispatch key IS the node_id by construction. Captured stdout/stderr are written
    there so the log dir is populated even if the script writes nothing itself.

    NO real model is invoked by this class; it only shells out to whatever script it is
    configured with. Real backoff/retry on a `busy` outcome is a later ladder step.
    """

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
        # A limit can land on either stream (grindstone greps both).
        if self._busy_re.search(stderr) or self._busy_re.search(stdout):
            return "busy"
        return "failed"

    def run(self, prompt: str, workdir: Path) -> Result:
        dispatch_dir = self.log_dir / Path(workdir).name
        dispatch_dir.mkdir(parents=True, exist_ok=True)
        prompt_file = dispatch_dir / "prompt.txt"
        prompt_file.write_text(prompt, encoding="utf-8")
        handle_out = dispatch_dir / "handle"

        argv = [
            str(self.script),
            "--worktree", str(workdir),
            "--prompt", str(prompt_file),
            "--log-dir", str(dispatch_dir),
            "--handle-out", str(handle_out),
            "--timeout", str(int(self.timeout_s)),
        ]
        proc_env = {**os.environ, **self.env}

        try:
            proc = subprocess.run(
                argv,
                cwd=workdir,
                capture_output=True,
                text=True,
                env=proc_env,
                timeout=self.timeout_s,
            )
        except subprocess.TimeoutExpired as exc:
            # Timeout is represented as a `failed` outcome with a "[timeout]" marker;
            # a dedicated timeout outcome is not warranted (the caller treats it like
            # any other non-busy failure). Reaping the process GROUP is ladder step 4.
            out = exc.stdout or b""
            err = exc.stderr or b""
            stdout = out.decode() if isinstance(out, bytes) else out
            stderr = err.decode() if isinstance(err, bytes) else err
            (dispatch_dir / "agent.stdout.log").write_text(stdout, encoding="utf-8")
            (dispatch_dir / "agent.stderr.log").write_text(stderr, encoding="utf-8")
            return Result(
                text=f"[timeout] script exceeded {self.timeout_s}s\n{stderr}",
                exit_code=None,
                outcome="failed",
            )

        (dispatch_dir / "agent.stdout.log").write_text(proc.stdout, encoding="utf-8")
        (dispatch_dir / "agent.stderr.log").write_text(proc.stderr, encoding="utf-8")

        outcome = self._classify(proc.returncode, proc.stdout, proc.stderr)
        text = proc.stdout if proc.returncode == 0 else (proc.stderr or proc.stdout)
        return Result(
            text=text,
            exit_code=proc.returncode,
            outcome=outcome,
        )


class RigRegistry:
    """Resolves a RigRef name to a Rig at execution time."""

    def __init__(self, rigs: dict[str, Rig]) -> None:
        self._rigs = dict(rigs)

    def __contains__(self, name: object) -> bool:
        return name in self._rigs

    def resolve(self, name: str) -> Rig:
        if name not in self._rigs:
            raise KeyError(f"unknown rig: {name!r}")
        return self._rigs[name]
