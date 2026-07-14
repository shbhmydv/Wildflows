"""Typed values shared by the v2 frame engine, rigs, and MCP boundary."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias

from pydantic import BaseModel, Field, field_validator

from wildflows.result import CommitReceipt

FrameOutcome: TypeAlias = Literal["ok", "failed", "busy", "refused"]
ToolName: TypeAlias = Literal["dispatch", "gate", "ask"]


class FrameResult(BaseModel):
    outcome: FrameOutcome = "ok"
    text: str = ""
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""


class DispatchRequest(BaseModel):
    tasks: list[str] = Field(min_length=1)
    rig: str = Field(min_length=1)
    parallel: bool = False

    @field_validator("tasks")
    @classmethod
    def _nonblank_tasks(cls, values: list[str]) -> list[str]:
        if any(not value.strip() for value in values):
            raise ValueError("dispatch tasks must be non-blank")
        return values


class GateRequest(BaseModel):
    cmd: str = Field(min_length=1)

    @field_validator("cmd")
    @classmethod
    def _nonblank_cmd(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("gate command must be non-blank")
        return value


class AskRequest(BaseModel):
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


ToolResponse: TypeAlias = DispatchResult | GateResult | AskResult


def call_hash(tool: ToolName, request: ToolRequest) -> str:
    """Hash canonical engine-validated call content, never client-provided bytes."""
    payload = {
        "tool": tool,
        "arguments": request.model_dump(mode="json"),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class FrameRuntime:
    """Ephemeral capabilities supplied to one resident frame process."""

    endpoint: str
    token: str
    frame_id: str
    shim_path: Path
    runtime_dir: Path
    next_call_index: int
