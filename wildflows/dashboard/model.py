"""Read-only projection of a v2 journal into dashboard view data."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import mimetypes
from pathlib import Path
from typing import cast

from fastapi import HTTPException, status

from wildflows.events import Event, FrameExited, FramePushed, parse_event
from wildflows.frame import DispatchRequest, FrameOutcome, GateResult, child_frame_id
from wildflows.projection import CallProjection, FrameProjection, RunProjection


@dataclass(frozen=True)
class WatchedRepo:
    """A repository exposed by one dashboard process."""

    repo_id: str
    name: str
    path: Path

    @classmethod
    def from_path(cls, path: Path) -> "WatchedRepo":
        resolved = Path(path).expanduser().resolve()
        digest = hashlib.sha256(str(resolved).encode("utf-8")).hexdigest()[:8]
        name = resolved.name or "repository"
        safe_name = "".join(char if char.isalnum() or char in "-_" else "-" for char in name)
        return cls(repo_id=f"{safe_name}-{digest}", name=name, path=resolved)

    def public(self) -> dict[str, object]:
        return {"id": self.repo_id, "name": self.name, "path": str(self.path)}


@dataclass(frozen=True)
class JournalSnapshot:
    projection: RunProjection
    events: list[Event]


class DashboardModel:
    """Resolve watched runs and derive UI state without mutating their journals."""

    def __init__(self, repos: list[Path]) -> None:
        watched: list[WatchedRepo] = []
        seen: set[Path] = set()
        for path in repos:
            repo = WatchedRepo.from_path(path)
            if repo.path in seen:
                continue
            seen.add(repo.path)
            watched.append(repo)
        if not watched:
            raise ValueError("dashboard requires at least one repository")
        self.repos = tuple(watched)
        self._by_id = {repo.repo_id: repo for repo in watched}

    def repo(self, repo_id: str) -> WatchedRepo:
        try:
            return self._by_id[repo_id]
        except KeyError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown repository") from exc

    def run_dir(self, repo_id: str, run_id: str) -> Path:
        repo = self.repo(repo_id)
        if not run_id or Path(run_id).name != run_id or run_id in (".", ".."):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown run")
        runs_dir = (repo.path / ".wildflows" / "runs").resolve(strict=False)
        candidate = (runs_dir / run_id).resolve(strict=False)
        if not candidate.is_relative_to(runs_dir) or not candidate.is_dir():
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown run")
        return candidate

    @staticmethod
    def snapshot(run_dir: Path) -> JournalSnapshot:
        projection = RunProjection()
        events: list[Event] = []
        path = run_dir / "events.ndjson"
        if not path.exists():
            return JournalSnapshot(projection, events)
        raw = path.read_bytes()
        complete = len(raw) if raw.endswith(b"\n") else raw.rfind(b"\n") + 1
        for position, line in enumerate(raw[:complete].splitlines()):
            decoded = json.loads(line)
            if not isinstance(decoded, dict):
                raise ValueError(f"journal record {position} is not an object")
            event = parse_event(cast(dict[str, object], decoded))
            if event.seq != position:
                raise ValueError(
                    f"non-contiguous journal seq {event.seq} at position {position}"
                )
            projection.apply(event)
            events.append(event)
        return JournalSnapshot(projection, events)

    @staticmethod
    def _run_state(projection: RunProjection) -> str:
        if projection.finished is not None:
            return "completed" if projection.finished.outcome == "ok" else "failed"
        if projection.pending_questions() or any(
            frame.relaunch_blocked is not None
            for frame in projection.frames.values()
        ):
            return "parked"
        return "running"

    def list_runs(self) -> dict[str, object]:
        values: list[dict[str, object]] = []
        for repo in self.repos:
            runs_dir = repo.path / ".wildflows" / "runs"
            if not runs_dir.is_dir():
                continue
            for path in runs_dir.iterdir():
                if not path.is_dir():
                    continue
                try:
                    snapshot = self.snapshot(path)
                    opened = snapshot.projection.opened
                    values.append({
                        "key": f"{repo.repo_id}:{path.name}",
                        "repo_id": repo.repo_id,
                        "repo_name": repo.name,
                        "repository": str(repo.path),
                        "run_id": path.name,
                        "run_short": path.name[:8],
                        "state": self._run_state(snapshot.projection),
                        "frames": len(snapshot.projection.frames),
                        "started_at": None if opened is None else opened.started_at,
                        "event_count": len(snapshot.events),
                    })
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    values.append({
                        "key": f"{repo.repo_id}:{path.name}",
                        "repo_id": repo.repo_id,
                        "repo_name": repo.name,
                        "repository": str(repo.path),
                        "run_id": path.name,
                        "run_short": path.name[:8],
                        "state": "invalid",
                        "frames": 0,
                        "started_at": None,
                        "event_count": 0,
                        "error": str(exc),
                    })
        def run_sort_key(item: dict[str, object]) -> tuple[float, str]:
            raw_started = item["started_at"]
            started_at = cast(float, raw_started) if raw_started is not None else -1.0
            return started_at, str(item["run_id"])

        values.sort(key=run_sort_key, reverse=True)
        return {
            "repositories": [repo.public() for repo in self.repos],
            "runs": values,
        }

    @staticmethod
    def _frame_path(frame_id: str) -> str:
        return " › ".join(frame_id.split("."))

    @staticmethod
    def _first_line(value: str, fallback: str = "—") -> str:
        for line in value.splitlines():
            if line.strip():
                return line.strip()
        return fallback

    @staticmethod
    def _times(events: list[Event]) -> tuple[dict[str, float], dict[str, float]]:
        started: dict[str, float] = {}
        ended: dict[str, float] = {}
        for event in events:
            if isinstance(event, FramePushed):
                started[event.frame_id] = event.ts
                ended.pop(event.frame_id, None)
            elif isinstance(event, FrameExited):
                ended[event.frame_id] = event.ts
        return started, ended

    @staticmethod
    def _pending_dispatch(frame_id: str, projection: RunProjection) -> CallProjection | None:
        pending = [
            call
            for call in projection.calls.values()
            if call.frame_id == frame_id and call.tool == "dispatch" and not call.completed
        ]
        return min(pending, key=lambda call: call.call_index) if pending else None

    @staticmethod
    def _pending_ask(frame_id: str, projection: RunProjection) -> CallProjection | None:
        pending = [
            call
            for call in projection.calls.values()
            if call.frame_id == frame_id and call.tool == "ask" and not call.completed
        ]
        return min(pending, key=lambda call: call.call_index) if pending else None

    @staticmethod
    def _own_outcomes(events: list[Event]) -> dict[str, FrameOutcome]:
        """Keep frame-exit outcomes separate from aggregate pop outcomes."""
        outcomes: dict[str, FrameOutcome] = {}
        for event in events:
            if isinstance(event, FrameExited):
                outcomes[event.frame_id] = event.outcome
        return outcomes

    def _frame_state(
        self,
        frame: FrameProjection,
        projection: RunProjection,
        own_outcomes: dict[str, FrameOutcome],
    ) -> str:
        own_outcome = own_outcomes.get(frame.frame_id)
        if own_outcome is not None:
            return "done" if own_outcome == "ok" else "failed"
        if frame.relaunch_blocked is not None:
            return "parked"
        if frame.waiting_for_slot:
            return "queued"
        if self._pending_ask(frame.frame_id, projection) is not None:
            return "parked"
        if self._pending_dispatch(frame.frame_id, projection) is not None:
            return "banked"
        return "running"

    def _call_data(
        self,
        call: CallProjection,
        projection: RunProjection,
        event_times: dict[int, float],
        own_outcomes: dict[str, FrameOutcome],
    ) -> dict[str, object]:
        children = sorted(
            (
                frame
                for frame in projection.frames.values()
                if frame.parent_frame_id == call.frame_id
                and frame.parent_call_index == call.call_index
            ),
            key=lambda frame: (
                frame.task_index if frame.task_index is not None else -1,
                frame.frame_id,
            ),
        )
        requested = len(call.request.tasks) if isinstance(call.request, DispatchRequest) else 0
        result = None if call.response is None else call.response.model_dump(mode="json")
        gate_language: str | None = None
        if isinstance(call.response, GateResult):
            disposition = "PASS" if call.response.exit_code == 0 else "FAIL"
            gate_language = f"gate: {disposition} (exit {call.response.exit_code})"
        started_at = event_times.get(call.started_seq)
        ended_at = event_times.get(call.finished_seq) if call.finished_seq >= 0 else None
        counts = Counter(
            self._frame_state(child, projection, own_outcomes) for child in children
        )
        return {
            "call_index": call.call_index,
            "tool": call.tool,
            "status": "completed" if call.completed else "pending",
            "call_hash": call.call_hash,
            "request": call.request.model_dump(mode="json"),
            "result": result,
            "gate_language": gate_language,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_s": (
                max(0.0, ended_at - started_at)
                if started_at is not None and ended_at is not None
                else None
            ),
            "children": [child.frame_id for child in children],
            "future_frame_ids": [
                child_frame_id(call.frame_id, call.call_index, task_index)
                for task_index in range(requested)
            ],
            "requested": requested,
            "queued": max(0, requested - len(children)),
            "parallel": (
                call.request.parallel if isinstance(call.request, DispatchRequest) else False
            ),
            "kinds": (
                list(call.request.kinds)
                if isinstance(call.request, DispatchRequest) else []
            ),
            "counts": dict(counts),
        }

    def _frame_data(
        self,
        frame: FrameProjection,
        projection: RunProjection,
        started: dict[str, float],
        ended: dict[str, float],
        event_times: dict[int, float],
        own_outcomes: dict[str, FrameOutcome],
    ) -> dict[str, object]:
        frame_started = started.get(frame.frame_id)
        frame_ended = ended.get(frame.frame_id)
        own_outcome = own_outcomes.get(frame.frame_id)
        calls = sorted(
            (
                call
                for call in projection.calls.values()
                if call.frame_id == frame.frame_id
            ),
            key=lambda call: call.call_index,
        )
        reason_source = frame.stderr or frame.text or (
            "frame exited without a result" if own_outcome is not None else ""
        )
        failed_children = sum(
            own_outcomes.get(child.frame_id) == "failed"
            for child in projection.frames.values()
            if child.parent_frame_id == frame.frame_id
        )
        return {
            "frame_id": frame.frame_id,
            "path": self._frame_path(frame.frame_id),
            "parent_frame_id": frame.parent_frame_id,
            "parent_call_index": frame.parent_call_index,
            "task_index": frame.task_index,
            "depth": frame.depth,
            "rig": frame.rig,
            "prompt": frame.prompt,
            "name": self._first_line(frame.prompt, frame.frame_id),
            "skills": list(frame.skills),
            "state": self._frame_state(frame, projection, own_outcomes),
            "outcome": own_outcome,
            "failed_children": failed_children,
            "text": frame.text,
            "stdout": frame.stdout,
            "stderr": frame.stderr,
            "exit_code": frame.exit_code,
            "reason": self._first_line(reason_source, ""),
            "started_at": frame_started,
            "ended_at": frame_ended,
            "duration_s": (
                max(0.0, frame_ended - frame_started)
                if frame_started is not None and frame_ended is not None
                else None
            ),
            "self_time_s": frame.self_time_s,
            "slot": frame.slot,
            "branch": frame.branch,
            "base_commit": frame.base_commit,
            "head": frame.head,
            "calls": [
                self._call_data(call, projection, event_times, own_outcomes)
                for call in calls
            ],
        }

    def _state_line(self, projection: RunProjection, frames: list[dict[str, object]]) -> str:
        root_id = projection.opened.root_frame_id if projection.opened is not None else "f0"
        blocked = sorted(
            (
                frame
                for frame in projection.frames.values()
                if frame.relaunch_blocked is not None
            ),
            key=lambda frame: (frame.depth, frame.frame_id),
        )
        if blocked:
            frame = blocked[0]
            return (
                f"{self._frame_path(frame.frame_id)} parked · "
                "unexplained frame branch advancement"
            )
        pending_asks = projection.pending_questions()
        if pending_asks:
            selected = pending_asks[0]
            return f"{self._frame_path(selected.frame_id)} parked for owner"
        banked = sorted(
            (
                (str(frame["frame_id"]), call)
                for frame in frames
                for call in cast(list[dict[str, object]], frame["calls"])
                if call["tool"] == "dispatch" and call["status"] == "pending"
            ),
            key=lambda value: (
                value[0].count("."),
                value[0],
                cast(int, value[1]["call_index"]),
            ),
        )
        active_leaves = [frame for frame in frames if frame["state"] == "running"]
        if banked:
            owner, call = banked[0]
            prefix = f"{self._frame_path(owner)} banked on call {call['call_index']}"
            if active_leaves:
                rigs = Counter(str(frame["rig"]) for frame in active_leaves)
                if len(rigs) == 1:
                    rig, count = next(iter(rigs.items()))
                    noun = "junior" if count == 1 else "juniors"
                    prefix += f" · {count} {rig} {noun} running"
                else:
                    prefix += f" · {len(active_leaves)} frames running"
            return prefix
        if projection.finished is not None:
            disposition = "finished" if projection.finished.outcome == "ok" else "failed"
            return f"{root_id} {disposition} · {len(frames)} frames"
        if active_leaves:
            return f"{self._frame_path(str(active_leaves[0]['frame_id']))} running"
        return f"{root_id} waiting for journal activity"

    def _artifacts(self, repo: WatchedRepo, run_id: str, run_dir: Path) -> list[dict[str, object]]:
        ignored_names = {"events.ndjson", "run.lock", "run.json"}
        values: list[dict[str, object]] = []
        for path in sorted(run_dir.rglob("*")):
            if not path.is_file() or path.is_symlink() or path.name in ignored_names:
                continue
            relative = path.relative_to(run_dir)
            if relative.parts and relative.parts[0] in {"answers", "runtime"}:
                continue
            mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            values.append({
                "path": relative.as_posix(),
                "name": path.name,
                "size": path.stat().st_size,
                "mime": mime,
                "url": (
                    f"/api/repos/{repo.repo_id}/runs/{run_id}/artifacts/"
                    f"{relative.as_posix()}"
                ),
            })
        return values

    def detail(self, repo_id: str, run_id: str, *, controls: bool = False) -> dict[str, object]:
        repo = self.repo(repo_id)
        run_dir = self.run_dir(repo_id, run_id)
        snapshot = self.snapshot(run_dir)
        projection = snapshot.projection
        if projection.opened is None:
            raise HTTPException(status.HTTP_409_CONFLICT, "run has no run_opened event")
        started, ended = self._times(snapshot.events)
        event_times = {event.seq: event.ts for event in snapshot.events}
        own_outcomes = self._own_outcomes(snapshot.events)
        frame_values = [
            self._frame_data(
                frame, projection, started, ended, event_times, own_outcomes
            )
            for frame in sorted(
                projection.frames.values(),
                key=lambda item: (item.depth, item.frame_id),
            )
        ]
        frames = {str(frame["frame_id"]): frame for frame in frame_values}
        state = self._run_state(projection)
        last_ts = snapshot.events[-1].ts if snapshot.events else projection.opened.started_at
        ended_at = projection.finished.ts if projection.finished is not None else None
        return {
            "key": f"{repo.repo_id}:{run_id}",
            "repo": repo.public(),
            "run_id": run_id,
            "run_short": run_id[:8],
            "state": state,
            "active": projection.finished is None,
            "started_at": projection.opened.started_at,
            "ended_at": ended_at,
            "journal_at": last_ts,
            "elapsed_s": max(
                0.0,
                (ended_at if ended_at is not None else last_ts)
                - projection.opened.started_at,
            ),
            "state_line": self._state_line(projection, frame_values),
            "root_frame_id": projection.opened.root_frame_id,
            "root_prompt": projection.opened.root_prompt,
            "run_branch": projection.opened.run_branch,
            "base_commit": projection.opened.base_commit,
            "policy": projection.opened.policy.model_dump(mode="json"),
            "frames": frames,
            "frame_order": [str(frame["frame_id"]) for frame in frame_values],
            "pending_questions": [
                {
                    "frame_id": call.frame_id,
                    "frame_path": self._frame_path(call.frame_id),
                    "call_index": call.call_index,
                    "question": call.request.model_dump(mode="json").get("question"),
                }
                for call in projection.pending_questions()
            ],
            "events": [event.model_dump(mode="json") for event in snapshot.events],
            "artifacts": self._artifacts(repo, run_id, run_dir),
            "controls": {"answer": controls},
        }

    def artifact(self, repo_id: str, run_id: str, relative_path: str) -> Path:
        run_dir = self.run_dir(repo_id, run_id)
        relative = Path(relative_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown artifact")
        candidate = (run_dir / relative).resolve(strict=False)
        allowed = {
            str(item["path"])
            for item in self._artifacts(self.repo(repo_id), run_id, run_dir)
        }
        if not candidate.is_relative_to(run_dir) or relative.as_posix() not in allowed:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown artifact")
        return candidate
