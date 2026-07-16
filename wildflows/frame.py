"""Typed values shared by the v2 frame engine, rigs, and MCP boundary."""
from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from wildflows.result import CommitReceipt

FrameOutcome: TypeAlias = Literal["ok", "failed", "busy", "refused"]
ToolName: TypeAlias = Literal["dispatch", "gate", "ask"]


class FrameResult(BaseModel):
    outcome: FrameOutcome = "ok"
    text: str = ""
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""


class _ToolRequestBase(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class DispatchRequest(_ToolRequestBase):
    tasks: list[str] = Field(min_length=1)
    rig: str | None = None
    parallel: bool = False
    skills: list[list[str]] = Field(default_factory=list)
    kinds: list[str] = Field(default_factory=list)

    @field_validator("tasks")
    @classmethod
    def _nonblank_tasks(cls, values: list[str]) -> list[str]:
        if any(not value.strip() for value in values):
            raise ValueError("dispatch tasks must be non-blank")
        return values

    @field_validator("rig")
    @classmethod
    def _nonblank_rig(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("dispatch rig must be non-blank")
        return value

    @model_validator(mode="after")
    def _per_task_fields(self) -> "DispatchRequest":
        # Omission is canonicalized to one empty bundle per task. This keeps an
        # omitted bundle and explicit no-skill bundles at one memoization identity.
        if not self.skills:
            self.skills = [[] for _ in self.tasks]
        if len(self.skills) != len(self.tasks):
            raise ValueError("dispatch skills must contain one list per task")
        if any(not name.strip() for bundle in self.skills for name in bundle):
            raise ValueError("dispatch skill names must be non-blank")
        if self.kinds and len(self.kinds) != len(self.tasks):
            raise ValueError("dispatch kinds must contain one string per task")
        if any(not kind.strip() for kind in self.kinds):
            raise ValueError("dispatch kinds must be non-blank")
        return self

    def skill_bundle(self, task_index: int) -> list[str]:
        return list(self.skills[task_index])


class GateRequest(_ToolRequestBase):
    cmd: str = Field(min_length=1)

    @field_validator("cmd")
    @classmethod
    def _nonblank_cmd(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("gate command must be non-blank")
        return value


class AskRequest(_ToolRequestBase):
    question: str = Field(min_length=1)

    @field_validator("question")
    @classmethod
    def _nonblank_question(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("ask question must be non-blank")
        return value


ToolRequest: TypeAlias = DispatchRequest | GateRequest | AskRequest


class ChildResult(BaseModel):
    frame_id: str
    outcome: FrameOutcome
    text: str = ""
    exit_code: int | None = None
    commits: list[CommitReceipt] = Field(default_factory=list)


class DispatchResult(BaseModel):
    outcome: FrameOutcome
    children: list[ChildResult] = Field(default_factory=list)
    error_code: str | None = None
    message: str | None = None

    def as_text(self) -> str:
        if self.outcome == "refused":
            return f"dispatch refused [{self.error_code}]: {self.message}"
        if not self.children and self.message:
            return self.message
        blocks = [
            f"[{child.frame_id}] {child.outcome} (exit={child.exit_code})\n{child.text}"
            for child in self.children
        ]
        return "\n\n".join(blocks)


class GateResult(BaseModel):
    exit_code: int
    stdout: str
    stderr: str

    def as_text(self) -> str:
        return (
            f"exit_code={self.exit_code}\n"
            f"--- stdout ---\n{self.stdout}\n"
            f"--- stderr ---\n{self.stderr}"
        )


class AskResult(BaseModel):
    answer: str

    def as_text(self) -> str:
        return self.answer


class ToolFailure(BaseModel):
    """Engine-owned durable failure for a validated call with no tool return."""

    outcome: Literal["failed"] = "failed"
    error_code: str
    message: str

    def as_text(self) -> str:
        return f"tool call failed [{self.error_code}]: {self.message}"


ToolResponse: TypeAlias = DispatchResult | GateResult | AskResult | ToolFailure


def call_hash(tool: ToolName, request: ToolRequest) -> str:
    """Hash canonical engine-validated call content, never client-provided bytes."""
    arguments = request.model_dump(mode="json")
    if isinstance(request, DispatchRequest):
        # Additive optional dispatch hints must not change old no-hint journal
        # identities when those calls are loaded and replayed.
        if not request.kinds:
            arguments.pop("kinds", None)
        if request.rig is None:
            arguments.pop("rig", None)
    payload = {
        "tool": tool,
        "arguments": arguments,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class WorkerLease(Protocol):
    """Engine-owned lifecycle record for one rig adapter process tree."""

    handle_path: Path

    def started(
        self,
        pid: int,
        process_group_id: int,
        session_id: int,
        start_time: int | None = None,
    ) -> None: ...

    def stop(self, reason: str) -> None: ...

    def finished(self) -> None: ...


@dataclass(frozen=True)
class FrameRuntime:
    """Ephemeral capabilities supplied to one resident frame process."""

    endpoint: str
    token: str
    frame_id: str
    shim_path: Path
    runtime_dir: Path
    next_call_index: int
    cancellation: threading.Event | None = None
    worker: WorkerLease | None = None
    environment: dict[str, str] | None = None
    backstop_timeout_s: float | None = None
