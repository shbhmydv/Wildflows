"""Root-frame run lifecycle; strategy lives in that frame, not a planner loop."""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterator
from uuid import uuid4

from wildflows.admission import AdmissionPolicy
from wildflows.engine import Engine
from wildflows.frame import FrameOutcome
from wildflows.rig import RigRegistry


@dataclass(frozen=True)
class RunCompleted:
    summary: str
    frames: int
    outcome: FrameOutcome


def _sync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with open(temporary, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    _sync_dir(path.parent)


class Run:
    """A durable job-spec-to-root-frame invocation."""

    def __init__(
        self,
        *,
        workdir: Path,
        job_spec: str | Path,
        registry: RigRegistry,
        root_rig: str,
        run_id: str | None = None,
        run_branch: str | None = None,
        policy: AdmissionPolicy | None = None,
        worktrees_root: Path | None = None,
    ) -> None:
        process = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=workdir,
            capture_output=True,
            text=True,
            check=True,
        )
        self.workdir = Path(process.stdout.strip()).resolve()
        self.run_id = run_id or uuid4().hex
        if (
            not self.run_id
            or self.run_id in (".", "..")
            or Path(self.run_id).name != self.run_id
        ):
            raise ValueError("run_id must be one path component")
        runs_dir = self.workdir / ".wildflows" / "runs"
        self.run_dir = runs_dir / self.run_id
        if not self.run_dir.resolve(strict=False).is_relative_to(
            runs_dir.resolve(strict=False)
        ):
            raise ValueError("target-local run directory escapes through a symlink")
        self.job = self._job_text(job_spec)
        self.root_rig = root_rig
        self._meta_path = self.run_dir / "run.json"
        self._completed_path = self.run_dir / "completed.json"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self._load_or_create_meta()
        self.engine = Engine(
            self.run_dir,
            self.workdir,
            registry,
            run_id=self.run_id,
            root_rig=root_rig,
            root_prompt=self.job,
            run_branch=run_branch,
            policy=policy,
            worktrees_root=worktrees_root,
        )

    @staticmethod
    def _job_text(value: str | Path) -> str:
        if isinstance(value, Path):
            return value.read_text(encoding="utf-8")
        if "\n" not in value:
            candidate = Path(value)
            if candidate.is_file():
                return candidate.read_text(encoding="utf-8")
        return value

    def _load_or_create_meta(self) -> None:
        if self._meta_path.exists():
            decoded = json.loads(self._meta_path.read_text(encoding="utf-8"))
            if not isinstance(decoded, dict):
                raise ValueError("durable run metadata is not an object")
            if decoded.get("job") != self.job:
                raise ValueError("resumed job spec differs from durable run job")
            if decoded.get("root_rig") != self.root_rig:
                raise ValueError("resumed root rig differs from durable run")
            return
        started_at = time.time()
        _atomic_write(
            self._meta_path,
            json.dumps({
                "run_id": self.run_id,
                "started_at": started_at,
                "job": self.job,
                "root_rig": self.root_rig,
            }, sort_keys=True).encode("utf-8"),
        )
        _atomic_write(self.run_dir / "job.md", self.job.encode("utf-8"))

    @contextmanager
    def _lifecycle_guard(self, *, blocking: bool = True) -> Iterator[bool]:
        descriptor = os.open(self.run_dir / "run.lock", os.O_RDWR | os.O_CREAT, 0o600)
        acquired = False
        try:
            operation = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
            try:
                fcntl.flock(descriptor, operation)
                acquired = True
            except BlockingIOError:
                yield False
                return
            yield True
        finally:
            if acquired:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def run(self) -> RunCompleted:
        with self._lifecycle_guard() as acquired:
            assert acquired
            return self._drive()

    def resume(
        self,
        *,
        answer: str | None = None,
        frame_id: str | None = None,
        call_index: int | None = None,
    ) -> RunCompleted:
        if answer is not None:
            self.engine.answer(answer, frame_id=frame_id, call_index=call_index)
            with self._lifecycle_guard(blocking=False) as acquired:
                if not acquired:
                    return RunCompleted(
                        summary="owner answer delivered; resident run continues",
                        frames=len(self.engine.projection.frames),
                        outcome="ok",
                    )
                return self._drive()
        return self.run()

    def _drive(self) -> RunCompleted:
        result = self.engine.run()
        completed = RunCompleted(
            summary=result.text,
            frames=len(self.engine.projection.frames),
            outcome=result.outcome,
        )
        _atomic_write(
            self._completed_path,
            json.dumps({
                "summary": completed.summary,
                "frames": completed.frames,
                "outcome": completed.outcome,
            }, sort_keys=True).encode("utf-8"),
        )
        return completed
