"""Local v2 frame-call-stack dashboard over one or more read-only journals."""
from __future__ import annotations

import secrets
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Query, status
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from wildflows.dashboard.model import DashboardModel
from wildflows.dashboard.sse import tail_events
from wildflows.run import Run


DEFAULT_PORT = 8181
_STATIC_DIR = Path(__file__).with_name("static")


class AnswerBody(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    answer: str = Field(min_length=1)
    frame_id: str
    call_index: int


def read_watchlist(path: Path) -> list[Path]:
    """Read a comment-friendly, one-repository-path-per-line watchlist."""
    source = path.expanduser().resolve()
    values: list[Path] = []
    for line in source.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        candidate = Path(value).expanduser()
        values.append(candidate if candidate.is_absolute() else source.parent / candidate)
    return values


def _event_cursor(after: int | None, last_event_id: str | None) -> int:
    cursor = -1 if after is None else after
    if last_event_id is None:
        return cursor
    try:
        return int(last_event_id)
    except ValueError as exc:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "Last-Event-ID must be an integer"
        ) from exc


def create_app(
    repos: Path | list[Path],
    *,
    control_token: str | None = None,
) -> FastAPI:
    paths = [repos] if isinstance(repos, Path) else list(repos)
    dashboard = DashboardModel(paths)
    app = FastAPI(title="wildflows frame console", version="2")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(_STATIC_DIR / "index.html")

    @app.get("/api/runs")
    def list_runs() -> dict[str, object]:
        return dashboard.list_runs()

    @app.get("/api/repos/{repo_id}/runs/{run_id}")
    def run_detail(repo_id: str, run_id: str) -> dict[str, object]:
        return dashboard.detail(repo_id, run_id, controls=control_token is not None)

    @app.get("/api/repos/{repo_id}/runs/{run_id}/events")
    def run_events(
        repo_id: str,
        run_id: str,
        after: Annotated[int | None, Query()] = None,
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        run_dir = dashboard.run_dir(repo_id, run_id)
        return StreamingResponse(
            tail_events(run_dir / "events.ndjson", _event_cursor(after, last_event_id)),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/api/repos/{repo_id}/runs/{run_id}/artifacts/{artifact_path:path}")
    def artifact(repo_id: str, run_id: str, artifact_path: str) -> FileResponse:
        return FileResponse(dashboard.artifact(repo_id, run_id, artifact_path))

    @app.post("/api/repos/{repo_id}/runs/{run_id}/answer")
    def answer(
        repo_id: str,
        run_id: str,
        body: AnswerBody,
        supplied_token: Annotated[
            str | None, Header(alias="X-Wildflows-Token")
        ] = None,
    ) -> dict[str, object]:
        if (
            control_token is None
            or supplied_token is None
            or not secrets.compare_digest(supplied_token, control_token)
        ):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "invalid control token")
        repo = dashboard.repo(repo_id)
        dashboard.run_dir(repo_id, run_id)
        try:
            delivered = Run.deliver_live_answer(
                repo.path,
                run_id,
                body.answer,
                frame_id=body.frame_id,
                call_index=body.call_index,
            )
        except (OSError, ValueError) as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        if not delivered:
            raise HTTPException(status.HTTP_409_CONFLICT, "run is not live")
        return {"delivered": True, "run_id": run_id}

    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")
    return app


def serve(
    repos: Path | list[Path],
    port: int = DEFAULT_PORT,
    *,
    watchlist: Path | None = None,
) -> None:
    import uvicorn

    paths = [repos] if isinstance(repos, Path) else list(repos)
    if watchlist is not None:
        paths.extend(read_watchlist(watchlist))
    if not paths:
        raise ValueError("dashboard requires --repo or --watchlist")
    token = secrets.token_urlsafe(24)
    print(f"wildflows dashboard: http://127.0.0.1:{port}")
    print(f"wildflows control token: {token}")
    uvicorn.run(create_app(paths, control_token=token), host="127.0.0.1", port=port)
