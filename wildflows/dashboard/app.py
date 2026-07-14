"""Minimal v2 dashboard backend: a journal consumer pending the frame-tree UI phase."""
from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from fastapi import FastAPI, HTTPException, status

from wildflows.events import Event, parse_event
from wildflows.projection import RunProjection


class Dashboard:
    def __init__(self, repo: Path) -> None:
        self.repo = Path(repo).resolve()
        self.runs_dir = self.repo / ".wildflows" / "runs"

    def run_dir(self, run_id: str) -> Path:
        if not run_id or Path(run_id).name != run_id or run_id in (".", ".."):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown run")
        candidate = (self.runs_dir / run_id).resolve(strict=False)
        if not candidate.is_relative_to(self.runs_dir.resolve(strict=False)) or not candidate.is_dir():
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown run")
        return candidate

    @staticmethod
    def snapshot(run_dir: Path) -> tuple[RunProjection, list[Event]]:
        projection = RunProjection()
        events: list[Event] = []
        path = run_dir / "events.ndjson"
        if not path.exists():
            return projection, events
        raw = path.read_bytes()
        complete = len(raw) if raw.endswith(b"\n") else raw.rfind(b"\n") + 1
        for position, line in enumerate(raw[:complete].splitlines()):
            decoded = json.loads(line)
            if not isinstance(decoded, dict):
                raise HTTPException(
                    status.HTTP_409_CONFLICT, f"invalid journal record {position}"
                )
            event = parse_event(cast(dict[str, object], decoded))
            if event.seq != position:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    f"non-contiguous journal seq {event.seq} at {position}",
                )
            projection.apply(event)
            events.append(event)
        return projection, events

    def detail(self, run_id: str) -> dict[str, object]:
        run_dir = self.run_dir(run_id)
        projection, events = self.snapshot(run_dir)
        pending = projection.pending_questions()
        state = "completed" if projection.finished is not None else (
            "parked" if pending else "running-or-stopped"
        )
        return {
            "run_id": run_id,
            "state": state,
            "frames": len(projection.frames),
            "pending_questions": [
                {
                    "frame_id": call.frame_id,
                    "call_index": call.call_index,
                    "question": call.request.model_dump(mode="json").get("question"),
                }
                for call in pending
            ],
            "events": [event.model_dump(mode="json") for event in events],
        }

    def list_runs(self) -> list[dict[str, object]]:
        if not self.runs_dir.is_dir():
            return []
        values: list[dict[str, object]] = []
        for path in sorted(self.runs_dir.iterdir(), reverse=True):
            if not path.is_dir():
                continue
            try:
                detail = self.detail(path.name)
                values.append({
                    "run_id": path.name,
                    "state": detail["state"],
                    "frames": detail["frames"],
                })
            except (HTTPException, ValueError, json.JSONDecodeError) as exc:
                values.append({"run_id": path.name, "state": "invalid", "error": str(exc)})
        return values


def create_app(repo: Path) -> FastAPI:
    dashboard = Dashboard(repo)
    app = FastAPI(title="wildflows v2 journal", version="2")

    @app.get("/api/runs")
    def list_runs() -> list[dict[str, object]]:
        return dashboard.list_runs()

    @app.get("/api/runs/{run_id}")
    def run_detail(run_id: str) -> dict[str, object]:
        return dashboard.detail(run_id)

    return app


def serve(repo: Path, port: int = 8765) -> None:
    import uvicorn

    uvicorn.run(create_app(repo), host="127.0.0.1", port=port)
