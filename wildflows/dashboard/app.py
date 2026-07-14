"""FastAPI dashboard: read-only journal projection plus CLI-backed controls."""
from __future__ import annotations
import asyncio
import fcntl
import json
import mimetypes
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import threading
import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any, BinaryIO, Iterator, cast
from urllib.parse import quote
from uuid import uuid4
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from wildflows.events import Event, parse_event
from wildflows.projection import NodeProjection, RunProjection
Json = dict[str, Any]
_PUBLIC_ROOTS = frozenset({"artifacts", "decisions", "handoffs"})
class ResumeRequest(BaseModel):
    rigs: str | None = None
    planner_rig: str | None = None
    max_workers: int | None = Field(default=None, ge=1)
    retry_setups: bool = False
class AnswerRequest(ResumeRequest):
    answer: str = Field(min_length=1)
    node_id: str | None = None
class LaunchRequest(ResumeRequest):
    job: str
    planner_rig: str = "planner"
    max_workers: int = Field(default=1, ge=1)
    run_id: str | None = None
    run_branch: str | None = None
@dataclass
class ManagedAction:
    action_id: str
    run_id: str
    kind: str
    log_path: Path
    process: subprocess.Popen[bytes]
    started_at: float
class ActionManager:
    def __init__(self, repo: Path) -> None:
        self.repo = repo
        self._actions: dict[str, ManagedAction] = {}
        self._lock = threading.Lock()
    def spawn(self, command: list[str], run_id: str, kind: str) -> ManagedAction:
        action_id = uuid4().hex
        directory = self.repo / ".wildflows" / "runs" / run_id / "control" / "actions"
        directory.mkdir(parents=True, exist_ok=True)
        log_path = directory / f"{action_id}.log"
        stream: BinaryIO = open(log_path, "ab", buffering=0)
        stream.write(f"$ wildflows dashboard action: {kind}\n".encode("utf-8"))
        try:
            process = subprocess.Popen(
                command,
                cwd=Path.cwd(),
                stdout=stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        finally:
            stream.close()
        action = ManagedAction(
            action_id, run_id, kind, log_path, process, time.time()
        )
        with self._lock:
            self._actions[action_id] = action
        threading.Thread(target=process.wait, daemon=True).start()
        return action
    def status(self, action_id: str) -> Json:
        with self._lock:
            action = self._actions.get(action_id)
        if action is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown action")
        returncode = action.process.poll()
        try:
            log = action.log_path.read_bytes()[-64_000:].decode("utf-8", "replace")
        except OSError:
            log = ""
        return {
            "action_id": action.action_id,
            "run_id": action.run_id,
            "kind": action.kind,
            "state": "running" if returncode is None else "finished",
            "returncode": returncode,
            "started_at": action.started_at,
            "log": log,
        }
class Dashboard:
    def __init__(self, repo: Path) -> None:
        process = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        )
        self.repo = Path(process.stdout.strip()).resolve()
        self.runs_dir = self.repo / ".wildflows" / "runs"
        self.actions = ActionManager(self.repo)
    def run_dir(self, run_id: str, *, must_exist: bool = True) -> Path:
        if not run_id or run_id in (".", "..") or Path(run_id).name != run_id:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown run")
        root = self.runs_dir.resolve(strict=False)
        candidate = (self.runs_dir / run_id).resolve(strict=False)
        if not candidate.is_relative_to(root) or (must_exist and not candidate.is_dir()):
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
        lines = raw[:complete].splitlines()
        for position, line in enumerate(lines):
            try:
                data = json.loads(line)
                if not isinstance(data, dict):
                    raise ValueError("event is not an object")
                event = parse_event(data)
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    f"invalid journal record {position}: {exc}",
                ) from exc
            if event.seq != position:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    f"non-contiguous journal seq {event.seq} at {position}",
                )
            projection.apply(event)
            events.append(event)
        return projection, events
    @staticmethod
    def _read_json(path: Path) -> Json:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, UnicodeError, json.JSONDecodeError):
            return {}
    @staticmethod
    def _process_row(pid: int) -> tuple[int, int, int, int] | None:
        try:
            tail = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1]
            fields = tail.split()
            return int(fields[1]), int(fields[2]), int(fields[3]), int(fields[19])
        except (OSError, UnicodeError, IndexError, ValueError):
            return None
    @classmethod
    def _active_live(cls, active: object) -> bool:
        if not isinstance(active, dict):
            return False
        pid, start = active.get("pid"), active.get("process_start")
        if not isinstance(pid, int) or not isinstance(start, int):
            return False
        row = cls._process_row(pid)
        return row is not None and row[3] == start
    @classmethod
    def _kill_identity(cls, active: object) -> tuple[int, int] | None:
        if not isinstance(active, dict):
            return None
        values = tuple(active.get(key) for key in ("pid", "pgid", "sid", "process_start"))
        if not all(isinstance(value, int) for value in values):
            return None
        pid, pgid, sid, started = cast(tuple[int, int, int, int], values)
        row = cls._process_row(pid)
        if row is None or row[3] != started or row[1] != pgid or row[2] != sid:
            return None
        return (pid, started) if pid == pgid == sid else None
    @staticmethod
    def _expr_nodes(expr: object) -> Iterator[Json]:
        if not isinstance(expr, dict):
            return
        yield expr
        children: object
        kind = expr.get("kind")
        if kind in ("seq", "dispatch"):
            children = expr.get("children", [])
        elif kind == "combine":
            children = expr.get("inputs", [])
        elif kind == "loop":
            children = [expr.get("body")]
        else:
            children = []
        if isinstance(children, list):
            for child in children:
                yield from Dashboard._expr_nodes(child)
    @staticmethod
    def _node_state(node: NodeProjection) -> str:
        if node.question is not None and node.asked_seq > node.answered_seq:
            return "parked-ask"
        if node.last_dispatch_seq > node.result_seq:
            return "running"
        if node.result is not None:
            if not node.result.ok:
                return "failed"
            if not node.receipt_required or node.integrated_seq > node.result_seq:
                return "integrated"
            return "running"
        if node.last_dispatch_seq >= 0:
            return "running"
        return "pending"
    def _file_info(self, run_id: str, run_dir: Path, relative: str) -> Json | None:
        try:
            path = self.public_file(run_dir, relative)
        except HTTPException:
            return None
        mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        return {
            "path": relative,
            "name": path.name,
            "mime": mime,
            "size": path.stat().st_size,
            "url": f"/api/runs/{quote(run_id)}/files/{quote(relative)}",
        }
    def _public_files(self, run_id: str, run_dir: Path) -> list[Json]:
        found: list[Json] = []
        for root_name in sorted(_PUBLIC_ROOTS):
            root = run_dir / root_name
            if (
                not root.is_dir()
                or not root.resolve().is_relative_to(run_dir.resolve())
            ):
                continue
            for path in sorted(root.rglob("*")):
                if len(found) >= 500 or not path.is_file():
                    continue
                try:
                    relative = path.relative_to(run_dir).as_posix()
                except ValueError:
                    continue
                item = self._file_info(run_id, run_dir, relative)
                if item is not None:
                    found.append(item)
        return found
    def detail(self, run_id: str) -> Json:
        run_dir = self.run_dir(run_id)
        projection, events = self.snapshot(run_dir)
        meta = self._read_json(run_dir / "run.json")
        completed = self._read_json(run_dir / "completed.json")
        active_value = meta.get("active")
        active = self._active_live(active_value)
        latest_id = max(projection.epochs, default=None)
        latest = projection.epochs.get(latest_id) if latest_id is not None else None
        pending = projection.pending_questions()
        open_epoch = latest is not None and latest.phase != "closed"
        rail_hit = False
        if latest is not None and latest.rails is not None:
            rails = latest.rails
            started = meta.get("started_at")
            if rails.deadline_s is not None and isinstance(started, (int, float)):
                rail_hit = time.time() >= float(started) + rails.deadline_s
        if completed:
            run_state = "completed"
        elif active:
            run_state = "running"
        elif pending:
            run_state = "parked"
        elif rail_hit:
            run_state = "railed"
        elif open_epoch and isinstance(active_value, dict):
            run_state = "crashed"
        elif open_epoch and any(
            value.result is not None and not value.result.ok
            for value in projection.nodes.values()
        ):
            run_state = "failed"
        elif open_epoch:
            run_state = "stopped"
        else:
            run_state = "ready"
        expr = latest.expr if latest is not None else None
        public_files = self._public_files(run_id, run_dir)
        nodes: dict[str, Json] = {}
        for raw in self._expr_nodes(expr):
            node_id = raw.get("node_id")
            if not isinstance(node_id, str):
                continue
            projected = projection.node((latest_id or 0, node_id))
            artifact_paths = {projected.artifact} if projected.artifact else set()
            prefix = f"artifacts/e{latest_id}-{node_id}/"
            artifacts = [
                item for item in public_files
                if item["path"] in artifact_paths or str(item["path"]).startswith(prefix)
            ]
            result = projected.result
            receipts = (
                [item.model_dump(mode="json") for item in projected.receipt.commits]
                if projected.receipt is not None else []
            )
            nodes[node_id] = {
                "node_id": node_id,
                "kind": raw.get("kind"),
                "state": self._node_state(projected),
                "task": raw.get("task") or raw.get("question") or raw.get("cmd"),
                "rig": (raw.get("rig") or {}).get("name")
                    if isinstance(raw.get("rig"), dict) else None,
                "expression": raw,
                "result": None if result is None else result.model_dump(mode="json"),
                "receipts": receipts,
                "artifact": projected.artifact,
                "artifacts": artifacts,
                "dispatch_count": projected.dispatch_count,
                "loop_status": projected.loop_status,
                "loop_iterations": projected.loop_iterations,
            }
        return {
            "run_id": run_id,
            "state": run_state,
            "started_at": meta.get("started_at"),
            "completed": completed or None,
            "active": active,
            "killable": self._kill_identity(active_value) is not None,
            "epoch": latest_id,
            "epoch_count": len(projection.epochs),
            "closed_epochs": sum(item.phase == "closed" for item in projection.epochs.values()),
            "rationale": latest.rationale if latest is not None else None,
            "rails": None if latest is None or latest.rails is None
                else latest.rails.model_dump(mode="json"),
            "expression": expr,
            "nodes": nodes,
            "pending_questions": [
                {
                    "epoch": item.epoch,
                    "node_id": item.node_id,
                    "question": item.question,
                    "options": list(item.options),
                }
                for item in pending
            ],
            "files": public_files,
            "events": [
                event.model_dump(mode="json", exclude_computed_fields=True)
                for event in events
            ],
        }
    def list_runs(self) -> list[Json]:
        if not self.runs_dir.is_dir():
            return []
        values: list[Json] = []
        for path in sorted(self.runs_dir.iterdir(), reverse=True):
            if not path.is_dir():
                continue
            try:
                detail = self.detail(path.name)
            except HTTPException as exc:
                values.append({"run_id": path.name, "state": "invalid", "error": exc.detail})
                continue
            values.append({
                key: detail[key]
                for key in ("run_id", "state", "started_at", "epoch_count", "rationale")
            })
        return values
    @staticmethod
    def public_file(run_dir: Path, relative: str) -> Path:
        candidate_rel = Path(relative)
        if (
            candidate_rel.is_absolute()
            or ".." in candidate_rel.parts
            or not candidate_rel.parts
            or candidate_rel.parts[0] not in _PUBLIC_ROOTS
        ):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "file is not public")
        root = run_dir.resolve()
        candidate = (run_dir / candidate_rel).resolve(strict=False)
        if not candidate.is_relative_to(root) or not candidate.is_file():
            raise HTTPException(status.HTTP_404_NOT_FOUND, "file is not public")
        return candidate
    def operator_path(self, value: str, what: str) -> Path:
        path = Path(value)
        candidate = (self.repo / path).resolve() if not path.is_absolute() else path.resolve()
        if not candidate.is_file():
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"{what} is not a file")
        return candidate
    def _config(self, run_dir: Path) -> Json:
        return self._read_json(run_dir / "control" / "config.json")
    def resume_command(self, run_id: str, body: ResumeRequest) -> list[str]:
        run_dir = self.run_dir(run_id)
        config = self._config(run_dir)
        job = run_dir / "job.md"
        if not job.is_file():
            raise HTTPException(status.HTTP_409_CONFLICT, "run has no durable job.md")
        rigs_value = body.rigs or config.get("rigs") or str(self.repo / "rigs.yaml")
        if not isinstance(rigs_value, str):
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid rigs path")
        rigs = self.operator_path(rigs_value, "rigs")
        planner = body.planner_rig or str(config.get("planner_rig", "planner"))
        configured_workers = config.get("max_workers", 1)
        workers = body.max_workers or (
            configured_workers if isinstance(configured_workers, int) else 1
        )
        command = [
            sys.executable, "-m", "wildflows", "resume", str(job),
            "--repo", str(self.repo), "--rigs", str(rigs),
            "--planner-rig", planner, "--run-id", run_id,
            "--max-workers", str(workers),
        ]
        projection, _ = self.snapshot(run_dir)
        branch: object = config.get("run_branch")
        if projection.epochs:
            branch = projection.epochs[max(projection.epochs)].run_branch or branch
        if isinstance(branch, str) and branch:
            command.extend(["--run-branch", branch])
        if body.retry_setups:
            command.append("--retry-setups")
        return command
    def launch(self, body: LaunchRequest) -> ManagedAction:
        job = self.operator_path(body.job, "job")
        rigs_value = body.rigs or str(job.parent / "rigs.yaml")
        rigs = self.operator_path(rigs_value, "rigs")
        run_id = body.run_id or uuid4().hex
        run_dir = self.run_dir(run_id, must_exist=False)
        if run_dir.exists() and any(run_dir.iterdir()):
            raise HTTPException(status.HTTP_409_CONFLICT, "run id already exists")
        config_dir = run_dir / "control"
        config_dir.mkdir(parents=True, exist_ok=True)
        config = {
            "rigs": str(rigs), "planner_rig": body.planner_rig,
            "max_workers": body.max_workers,
        }
        (config_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")
        command = [
            sys.executable, "-m", "wildflows", "run", str(job),
            "--repo", str(self.repo), "--rigs", str(rigs),
            "--planner-rig", body.planner_rig, "--run-id", run_id,
            "--max-workers", str(body.max_workers),
        ]
        if body.run_branch:
            command.extend(["--run-branch", body.run_branch])
        return self.actions.spawn(command, run_id, "launch")
    @staticmethod
    def _lock_held(path: Path) -> bool:
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return False
        finally:
            os.close(descriptor)
    @classmethod
    def _descendant_identities(cls, root_pid: int) -> dict[int, int]:
        rows: dict[int, tuple[int, int, int, int]] = {}
        try:
            entries = list(Path("/proc").iterdir())
        except OSError:
            entries = []
        for entry in entries:
            if entry.name.isdigit() and (row := cls._process_row(int(entry.name))) is not None:
                rows[int(entry.name)] = row
        members = {root_pid}
        while True:
            added = {pid for pid, row in rows.items() if row[0] in members} - members
            if not added:
                break
            members.update(added)
        return {pid: rows[pid][3] for pid in members if pid in rows}
    @classmethod
    def _signal_identity(cls, pid: int, started: int, which: signal.Signals) -> bool:
        row = cls._process_row(pid)
        if row is None or row[3] != started:
            return False
        try:
            descriptor = os.pidfd_open(pid)
        except (AttributeError, OSError):
            return False
        try:
            row = cls._process_row(pid)
            if row is None or row[3] != started:
                return False
            signal.pidfd_send_signal(descriptor, which)
            return True
        except ProcessLookupError:
            return False
        finally:
            os.close(descriptor)
    def kill(self, run_id: str) -> Json:
        run_dir = self.run_dir(run_id)
        if not self._lock_held(run_dir / "run.lock"):
            raise HTTPException(status.HTTP_409_CONFLICT, "run has no active lock holder")
        identity = self._kill_identity(self._read_json(run_dir / "run.json").get("active"))
        if identity is None:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "active process is stale or not a dashboard-managed session",
            )
        pid, started = identity
        members = self._descendant_identities(pid)
        ordered = [item for item in members.items() if item[0] != pid] + [(pid, started)]
        for member, generation in ordered:
            self._signal_identity(member, generation, signal.SIGTERM)
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and any(
            (row := self._process_row(member)) is not None and row[3] == generation
            for member, generation in members.items()
        ):
            time.sleep(0.025)
        for member, generation in ordered:
            self._signal_identity(member, generation, signal.SIGKILL)
        return {"run_id": run_id, "killed": True, "pid": pid, "processes": sorted(members)}
async def _tail_events(
    request: Request, path: Path, after: int
) -> AsyncGenerator[str, None]:
    offset = 0
    last = after
    heartbeat = time.monotonic()
    while True:
        if await request.is_disconnected():
            return
        try:
            size = path.stat().st_size
            if size < offset:
                offset = 0
            with open(path, "rb") as stream:
                stream.seek(offset)
                while True:
                    line = stream.readline()
                    if not line or not line.endswith(b"\n"):
                        break
                    offset = stream.tell()
                    try:
                        data = json.loads(line)
                        seq = data.get("seq") if isinstance(data, dict) else None
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if isinstance(seq, int) and seq > last:
                        last = seq
                        payload = json.dumps(data, separators=(",", ":"))
                        yield f"id: {seq}\nevent: journal\ndata: {payload}\n\n"
                        heartbeat = time.monotonic()
        except FileNotFoundError:
            pass
        if time.monotonic() - heartbeat >= 15:
            yield ": heartbeat\n\n"
            heartbeat = time.monotonic()
        await asyncio.sleep(0.2)
def create_app(repo: Path, token: str | None = None) -> FastAPI:
    dashboard = Dashboard(repo)
    control_token = token or secrets.token_urlsafe(24)
    static = Path(__file__).with_name("static")
    app = FastAPI(title="WILDFLOWS Dashboard", docs_url=None, redoc_url=None)
    app.state.dashboard = dashboard
    app.state.control_token = control_token
    app.mount("/static", StaticFiles(directory=static), name="static")
    def authorize(x_wildflows_token: str | None = Header(default=None)) -> None:
        supplied = x_wildflows_token or ""
        if not secrets.compare_digest(supplied, control_token):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "invalid control token")
    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(static / "index.html")
    @app.get("/api/runs")
    def runs() -> Json:
        return {"runs": dashboard.list_runs()}
    @app.get("/api/runs/{run_id}")
    def run_detail(run_id: str) -> Json:
        return dashboard.detail(run_id)
    @app.get("/api/runs/{run_id}/events")
    def journal_events(
        request: Request,
        run_id: str,
        after: int = -1,
        last_event_id: str | None = Header(default=None),
    ) -> StreamingResponse:
        run_dir = dashboard.run_dir(run_id)
        if last_event_id is not None:
            try:
                after = max(after, int(last_event_id))
            except ValueError:
                pass
        return StreamingResponse(
            _tail_events(request, run_dir / "events.ndjson", after),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    @app.get("/api/runs/{run_id}/files/{relative:path}")
    def file(run_id: str, relative: str) -> FileResponse:
        run_dir = dashboard.run_dir(run_id)
        path = dashboard.public_file(run_dir, relative)
        headers = {
            "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": (
                "sandbox; default-src 'none'; style-src 'unsafe-inline'; img-src data:"
            ),
        }
        return FileResponse(
            path, filename=None, content_disposition_type="inline", headers=headers
        )
    @app.get("/api/actions/{action_id}")
    def action_status(action_id: str) -> Json:
        return dashboard.actions.status(action_id)
    @app.post("/api/runs", status_code=status.HTTP_202_ACCEPTED)
    def launch(body: LaunchRequest, _: None = Depends(authorize)) -> Json:
        action = dashboard.launch(body)
        return {"run_id": action.run_id, "action_id": action.action_id}
    @app.post("/api/runs/{run_id}/resume", status_code=status.HTTP_202_ACCEPTED)
    def resume(run_id: str, body: ResumeRequest, _: None = Depends(authorize)) -> Json:
        command = dashboard.resume_command(run_id, body)
        action = dashboard.actions.spawn(command, run_id, "resume")
        return {"run_id": run_id, "action_id": action.action_id}
    @app.post("/api/runs/{run_id}/answer", status_code=status.HTTP_202_ACCEPTED)
    def answer(run_id: str, body: AnswerRequest, _: None = Depends(authorize)) -> Json:
        command = dashboard.resume_command(run_id, body)
        command.extend(["--answer", body.answer])
        if body.node_id:
            command.extend(["--answer-node", body.node_id])
        action = dashboard.actions.spawn(command, run_id, "answer")
        return {"run_id": run_id, "action_id": action.action_id}
    @app.post("/api/runs/{run_id}/kill")
    def kill(run_id: str, _: None = Depends(authorize)) -> Json:
        return dashboard.kill(run_id)
    return app
def serve(repo: Path, port: int = 8765) -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - exercised by base-only installs
        raise RuntimeError("dashboard dependencies missing; install wildflows[dash]") from exc
    token = secrets.token_urlsafe(24)
    print(f"WILDFLOWS dashboard http://127.0.0.1:{port}", flush=True)
    print(f"control token: {token}", flush=True)
    uvicorn.run(create_app(repo, token), host="127.0.0.1", port=port, log_level="info")
