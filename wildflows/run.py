"""Root-frame run lifecycle; strategy lives in that frame, not a planner loop."""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from uuid import uuid4

from wildflows.admission import AdmissionPolicy
from wildflows.engine import Engine, FrameCallJoinTimeoutError
from wildflows.events import parse_event
from wildflows.frame import FrameOutcome
from wildflows.projection import RunProjection
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


class LifecycleLockError(RuntimeError):
    """Another process owns this run's repair-capable lifecycle."""


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
        notify_command: list[str] | None = None,
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
        self._lifecycle_descriptor: int | None = None
        self._acquire_lifecycle()
        try:
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
                notify_command=notify_command,
            )
        except BaseException:
            self._release_lifecycle()
            raise

    @staticmethod
    def deliver_live_answer(
        workdir: Path,
        run_id: str,
        answer: str,
        *,
        frame_id: str | None = None,
        call_index: int | None = None,
    ) -> bool:
        """Wake a live parked run without constructing a repair-capable Journal."""
        run_dir = Path(workdir).resolve() / ".wildflows" / "runs" / run_id
        descriptor = os.open(run_dir / "run.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                projection = RunProjection()
                raw = (run_dir / "events.ndjson").read_bytes()
                complete = len(raw) if raw.endswith(b"\n") else raw.rfind(b"\n") + 1
                for line in raw[:complete].splitlines():
                    decoded = json.loads(line)
                    if not isinstance(decoded, dict):
                        raise ValueError("journal event is not an object")
                    projection.apply(parse_event(cast(dict[str, object], decoded)))
                selected = [
                    call
                    for call in projection.pending_questions()
                    if (frame_id is None or call.frame_id == frame_id)
                    and (call_index is None or call.call_index == call_index)
                ]
                if len(selected) != 1:
                    raise ValueError(
                        f"answer target is ambiguous or absent ({len(selected)} matches)"
                    )
                call = selected[0]
                answers = run_dir / "answers"
                answers.mkdir(parents=True, exist_ok=True, mode=0o700)
                os.chmod(answers, 0o700)
                safe = call.frame_id.replace("/", "-")
                path = answers / f"{safe}-{call.call_index}.txt"
                temporary = answers / f".{path.name}.{uuid4().hex}.tmp"
                descriptor_out = os.open(
                    temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                )
                try:
                    with os.fdopen(descriptor_out, "w", encoding="utf-8") as stream:
                        stream.write(answer)
                        stream.flush()
                        os.fsync(stream.fileno())
                    try:
                        os.link(temporary, path)
                    except FileExistsError as exc:
                        raise ValueError("owner question already has an answer") from exc
                finally:
                    temporary.unlink(missing_ok=True)
                _sync_dir(answers)
                return True
            else:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                return False
        finally:
            os.close(descriptor)

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

    def _acquire_lifecycle(self) -> None:
        descriptor = os.open(self.run_dir / "run.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise LifecycleLockError(
                f"run {self.run_id!r} is owned by another lifecycle"
            ) from exc
        self._lifecycle_descriptor = descriptor

    def _release_lifecycle(self) -> None:
        descriptor = getattr(self, "_lifecycle_descriptor", None)
        if descriptor is None:
            return
        self._lifecycle_descriptor = None
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    def close(self) -> None:
        """Release a constructed run that will not be driven."""
        self._release_lifecycle()

    def __del__(self) -> None:
        self._release_lifecycle()

    def run(self) -> RunCompleted:
        if self._lifecycle_descriptor is None:
            raise LifecycleLockError("run lifecycle is no longer owned")
        release_lifecycle = True
        try:
            return self._drive()
        except FrameCallJoinTimeoutError:
            # The uncooperative worker still belongs to this live Engine. Keep
            # cross-process lifecycle ownership until the caller retries/closes
            # or this process exits and takes its process groups with it.
            release_lifecycle = False
            raise
        finally:
            if release_lifecycle:
                self._release_lifecycle()

    def resume(
        self,
        *,
        answer: str | None = None,
        frame_id: str | None = None,
        call_index: int | None = None,
    ) -> RunCompleted:
        if answer is not None:
            self.engine.answer(answer, frame_id=frame_id, call_index=call_index)
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
