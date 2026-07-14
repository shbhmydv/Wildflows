"""Planner-driven run loop: durable epoch expressions, bounded context, and rails."""
from __future__ import annotations
import json
import os
from dataclasses import dataclass
from pathlib import Path
import subprocess
import time
from typing import Any
from uuid import uuid4
from pydantic import ValidationError
from wildflows.admission import AdmissionError
from wildflows.engine import Engine
from wildflows.events import Answered, ResultEvent
from wildflows.expr import Expr, parse_expr
from wildflows.planner import PlannerDecision, PlannerFailure, RailStop, Rails
from wildflows.rig import RigRegistry
@dataclass(frozen=True)
class RunCompleted:
    summary: str
    epochs: int
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
    """A job-spec-to-completion loop whose planner is an ordinary registered rig."""
    def __init__(
        self,
        *,
        workdir: Path,
        job_spec: str | Path,
        registry: RigRegistry,
        planner_rig: str,
        run_id: str | None = None,
        run_branch: str | None = None,
        max_workers: int = 1,
    ) -> None:
        root = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=workdir,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        self.workdir = Path(root).resolve()
        self.run_id = run_id or uuid4().hex
        if not self.run_id or Path(self.run_id).name != self.run_id:
            raise ValueError("run_id must be one path component")
        self.run_dir = self.workdir / ".wildflows" / "runs" / self.run_id
        self.registry = registry
        self.planner_rig = planner_rig
        self.job = self._job_text(job_spec)
        self._meta_path = self.run_dir / "run.json"
        self._completed_path = self.run_dir / "completed.json"
        self._load_or_create_meta()
        self.engine = Engine(
            self.run_dir,
            self.workdir,
            registry,
            run_branch=run_branch,
            max_workers=max_workers,
        )
    @staticmethod
    def _job_text(value: str | Path) -> str:
        if isinstance(value, Path):
            return value.read_text(encoding="utf-8")
        candidate = Path(value)
        if "\n" not in value and candidate.is_file():
            return candidate.read_text(encoding="utf-8")
        return value
    def _load_or_create_meta(self) -> None:
        if self._meta_path.exists():
            data = json.loads(self._meta_path.read_text(encoding="utf-8"))
            if data["job"] != self.job:
                raise ValueError("resumed job spec differs from the durable run job")
            self.started_at = float(data["started_at"])
            self.rails = Rails.model_validate(data.get("rails", {}))
            return
        self.started_at = time.time()
        self.rails = Rails()
        self._save_meta()
        _atomic_write(self.run_dir / "job.md", self.job.encode("utf-8"))
    def _save_meta(self) -> None:
        data = {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "job": self.job,
            "rails": self.rails.model_dump(mode="json"),
        }
        _atomic_write(
            self._meta_path,
            json.dumps(data, sort_keys=True).encode("utf-8"),
        )
    def run(
        self,
        *,
        answer: str | None = None,
        answer_file: Path | None = None,
        answer_node: str | None = None,
    ) -> RunCompleted:
        return self._drive(answer=answer, answer_file=answer_file, answer_node=answer_node)
    def resume(
        self,
        *,
        answer: str | None = None,
        answer_file: Path | None = None,
        answer_node: str | None = None,
    ) -> RunCompleted:
        return self._drive(answer=answer, answer_file=answer_file, answer_node=answer_node)
    def _drive(
        self,
        *,
        answer: str | None,
        answer_file: Path | None,
        answer_node: str | None,
    ) -> RunCompleted:
        if answer is not None and answer_file is not None:
            raise ValueError("pass answer or answer_file, not both")
        if answer_file is not None:
            answer = answer_file.read_text(encoding="utf-8")
        if answer is not None:
            self.engine.answer(answer, node_id=answer_node)
        while True:
            completed = self._completed()
            if completed is not None:
                return completed
            projection = self.engine.journal.projection
            open_epochs = [
                epoch for epoch in projection.epochs
                if not projection.epoch_closed(epoch)
            ]
            if open_epochs:
                epoch = max(open_epochs)
                expression = projection.epoch_expr(epoch)
                if expression is None:
                    raise RuntimeError(f"open epoch {epoch} has no durable expression")
                rails = projection.epoch_rails(epoch) or self.rails
                self._enforce_rails(epoch, rails, check_max=False)
                self.engine.run_epoch(
                    parse_expr(expression),
                    epoch,
                    rails=rails,
                    rationale=projection.epochs[epoch].rationale,
                    deadline_at=self._deadline_at(rails),
                )
                continue
            completed_epochs = len([
                value for value in projection.epochs.values() if value.phase == "closed"
            ])
            epoch = max(projection.epochs, default=-1) + 1
            self._enforce_rails(epoch, self.rails, check_max=True)
            decision, decision_path = self._plan(epoch)
            try:
                tree = self._validate_decision(decision)
                self.rails = self._updated_rails(decision.rails)
            except (ValueError, ValidationError, AdmissionError) as exc:
                raise PlannerFailure(f"planner decision rejected: {exc}", decision_path) from exc
            self._save_meta()
            if decision.end:
                assert decision.final_summary is not None
                completed = RunCompleted(decision.final_summary, completed_epochs)
                _atomic_write(
                    self._completed_path,
                    json.dumps({
                        "summary": completed.summary,
                        "epochs": completed.epochs,
                        "decision": decision_path.name,
                    }, sort_keys=True).encode("utf-8"),
                )
                return completed
            assert tree is not None
            self._enforce_rails(epoch, self.rails, check_max=True)
            try:
                self.engine.run_epoch(
                    tree,
                    epoch,
                    rails=self.rails,
                    rationale=decision.rationale,
                    deadline_at=self._deadline_at(self.rails),
                )
            except AdmissionError as exc:
                raise PlannerFailure(
                    f"planner expression failed admission: {exc}", decision_path
                ) from exc
    def _completed(self) -> RunCompleted | None:
        if not self._completed_path.exists():
            return None
        data = json.loads(self._completed_path.read_text(encoding="utf-8"))
        return RunCompleted(summary=str(data["summary"]), epochs=int(data["epochs"]))
    def _plan(self, epoch: int) -> tuple[PlannerDecision, Path]:
        prompt = self._planner_prompt(epoch)
        directory = self.run_dir / "decisions"
        attempt = len(list(directory.glob(f"epoch-{epoch:04d}-attempt-*.json"))) if directory.exists() else 0
        path = directory / f"epoch-{epoch:04d}-attempt-{attempt:03d}.json"
        try:
            result = self.registry.resolve(self.planner_rig).run(prompt, self.workdir)
        except Exception as exc:
            _atomic_write(path, b"")
            raise PlannerFailure(f"planner rig failed: {exc}", path) from exc
        _atomic_write(path, result.text.encode("utf-8"))
        if not result.ok:
            raise PlannerFailure(
                f"planner rig returned {result.outcome}: {result.text[-1000:]}", path
            )
        try:
            raw = json.loads(result.text)
            return PlannerDecision.model_validate(raw), path
        except (json.JSONDecodeError, ValidationError, TypeError) as exc:
            raise PlannerFailure(f"malformed planner decision: {exc}", path) from exc
    @staticmethod
    def _validate_decision(decision: PlannerDecision) -> Expr | None:
        if decision.end:
            return None
        assert decision.expression is not None
        return parse_expr(decision.expression)
    def _updated_rails(self, update: Rails) -> Rails:
        deadline = update.deadline_s if update.deadline_s is not None else self.rails.deadline_s
        if (
            self.rails.deadline_s is not None
            and deadline is not None
            and deadline > self.rails.deadline_s
        ):
            raise ValueError("deadline_s may only move downward")
        return Rails(
            deadline_s=deadline,
            max_epochs=(
                update.max_epochs
                if update.max_epochs is not None
                else self.rails.max_epochs
            ),
            budget_notes=update.budget_notes or self.rails.budget_notes,
        )
    def _deadline_at(self, rails: Rails) -> float | None:
        return None if rails.deadline_s is None else self.started_at + rails.deadline_s
    def _enforce_rails(self, epoch: int, rails: Rails, *, check_max: bool) -> None:
        elapsed = time.time() - self.started_at
        if rails.deadline_s is not None and elapsed >= rails.deadline_s:
            raise RailStop(
                run_id=self.run_id,
                epoch=epoch,
                rail="deadline_s",
                limit=rails.deadline_s,
                observed=elapsed,
            )
        if check_max and rails.max_epochs is not None and epoch >= rails.max_epochs:
            raise RailStop(
                run_id=self.run_id,
                epoch=epoch,
                rail="max_epochs",
                limit=float(rails.max_epochs),
                observed=float(epoch),
            )
    def _planner_prompt(self, epoch: int) -> str:
        elapsed = time.time() - self.started_at
        rails_state = {
            **self.rails.model_dump(mode="json"),
            "elapsed_s": round(elapsed, 3),
            "epoch": epoch,
        }
        macros = self._macros()
        return (
            "You are the WILDFLOWS planner. Return exactly one JSON object matching "
            "PlannerDecision: expression (expression JSON or null), rails "
            "{deadline_s?, max_epochs?, budget_notes?}, rationale, end, and "
            "final_summary when ending. No markdown fences.\n\n"
            f"## Job spec\n{self.job}\n\n"
            f"## Prior epoch digest\n{json.dumps(self._prior_digest(epoch), separators=(',', ':'))}\n\n"
            f"## Macro library (nudges only; emit the expanded expression yourself)\n"
            f"{json.dumps(macros, separators=(',', ':'))}\n\n"
            f"## Rails state\n{json.dumps(rails_state, separators=(',', ':'))}\n\n"
            f"Full run artifacts: {self.run_dir}\n"
        )
    def _prior_digest(self, epoch: int) -> dict[str, Any] | None:
        if epoch == 0:
            return None
        latest: dict[str, dict[str, Any]] = {}
        remaining = 16_000
        for event in self.engine.journal.projection.effective_events:
            if event.epoch != epoch - 1 or not isinstance(event, (ResultEvent, Answered)):
                continue
            text = event.text if isinstance(event, ResultEvent) else event.answer
            preview = text[: min(2_000, remaining)]
            remaining -= len(preview)
            latest[event.node_id] = {
                "node_id": event.node_id,
                "outcome": (
                    event.outcome if isinstance(event, ResultEvent)
                    else ("ok" if event.ok else "failed")
                ),
                "text": preview,
                "text_chars": len(text),
                "text_truncated": len(preview) < len(text),
                "paths": event.files[:25] if isinstance(event, ResultEvent) else [],
                "paths_truncated": isinstance(event, ResultEvent) and len(event.files) > 25,
                "artifact": event.artifact if isinstance(event, ResultEvent) else None,
            }
        nodes = list(latest.values())
        return {
            "epoch": epoch - 1,
            "nodes": nodes[:100],
            "node_count": len(nodes),
            "nodes_truncated": len(nodes) > 100,
        }
    def _macros(self) -> list[dict[str, str]]:
        roots = [Path(__file__).with_name("macros"), self.run_dir.parent / "macros"]
        found: dict[str, dict[str, str]] = {}
        for root in roots:
            if not root.is_dir():
                continue
            for path in sorted(root.glob("*.json")):
                data = json.loads(path.read_text(encoding="utf-8"))
                name, description = data.get("name"), data.get("description")
                if isinstance(name, str) and isinstance(description, str):
                    found[name] = {
                        "name": name,
                        "description": description,
                        "path": str(path),
                    }
        return [found[name] for name in sorted(found)]
