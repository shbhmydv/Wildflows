"""The frozen, incompatible v2 frame-journal vocabulary."""
from __future__ import annotations

import time
from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, Field, TypeAdapter

from wildflows.admission import AdmissionPolicy
from wildflows.frame import (
    AskRequest,
    DispatchRequest,
    DispatchResult,
    FrameOutcome,
    GateRequest,
    GateResult,
    ToolFailure,
    ToolName,
    ToolRequest,
)
from wildflows.result import CommitReceipt


class _Header(BaseModel):
    version: Literal[2] = 2
    seq: int = -1
    ts: float = Field(default_factory=time.time)
    run_id: str


class RunOpened(_Header):
    kind: Literal["run_opened"] = "run_opened"
    repository: str
    run_branch: str
    base_commit: str
    root_frame_id: str
    root_rig: str
    root_prompt: str
    worktrees_root: str
    worktree_setup: str | None = None
    worktree_links: list[str] = Field(default_factory=list)
    started_at: float
    policy: AdmissionPolicy


class FramePushed(_Header):
    kind: Literal["frame_pushed"] = "frame_pushed"
    frame_id: str
    parent_frame_id: str | None = None
    parent_call_index: int | None = None
    task_index: int | None = None
    attempt: int
    depth: int
    rig: str
    prompt: str
    skills: list[str] = Field(default_factory=list)
    branch: str
    base_commit: str
    worktree: str
    subtree_deadline: float


class WorktreeProvisioned(_Header):
    """One configured provisioning mechanism completed for a frame checkout."""

    kind: Literal["worktree_provisioned"] = "worktree_provisioned"
    frame_id: str
    attempt: int
    worktree: str
    mechanism: Literal["setup", "link"]
    duration_s: float = Field(ge=0)
    outcome: Literal["ok", "failed", "skipped"]
    output_tail: str = ""
    linked: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class FrameSlotQueued(_Header):
    """A pushed frame is waiting for one configured active rig slot."""

    kind: Literal["frame_slot_queued"] = "frame_slot_queued"
    frame_id: str
    attempt: int
    rig: str


class FrameSlotAcquired(_Header):
    """A frame's self-time clock started with an active rig lease."""

    kind: Literal["frame_slot_acquired"] = "frame_slot_acquired"
    frame_id: str
    attempt: int
    rig: str
    slot: int | None = None


class FrameSlotReleased(_Header):
    """A frame parked or exited and stopped consuming self-time and capacity."""

    kind: Literal["frame_slot_released"] = "frame_slot_released"
    frame_id: str
    attempt: int
    rig: str
    slot: int | None = None
    active_s: float = Field(ge=0)
    reason: str


class DispatchCalled(_Header):
    kind: Literal["dispatch_called"] = "dispatch_called"
    frame_id: str
    call_index: int
    call_hash: str
    request: DispatchRequest
    caller_head: str


class DispatchReturned(_Header):
    kind: Literal["dispatch_returned"] = "dispatch_returned"
    frame_id: str
    call_index: int
    call_hash: str
    result: DispatchResult


class GateCalled(_Header):
    kind: Literal["gate_called"] = "gate_called"
    frame_id: str
    call_index: int
    call_hash: str
    request: GateRequest
    caller_head: str


class GateReturned(_Header):
    kind: Literal["gate_returned"] = "gate_returned"
    frame_id: str
    call_index: int
    call_hash: str
    result: GateResult


class Asked(_Header):
    kind: Literal["asked"] = "asked"
    frame_id: str
    call_index: int
    call_hash: str
    request: AskRequest
    caller_head: str | None = None


class Answered(_Header):
    kind: Literal["answered"] = "answered"
    frame_id: str
    call_index: int
    call_hash: str
    answer: str


class CallFailed(_Header):
    """A validated call stopped without producing its tool-specific return."""

    kind: Literal["call_failed"] = "call_failed"
    frame_id: str
    call_index: int
    call_hash: str
    tool: ToolName
    request: ToolRequest
    result: ToolFailure


class WorkerReaped(_Header):
    """One engine-owned rig process tree was signalled and confirmed stopped."""

    kind: Literal["worker_reaped"] = "worker_reaped"
    frame_id: str
    attempt: int
    pid: int
    process_group_id: int
    session_id: int
    reason: str
    escalated: bool


class FrameRelaunchBlocked(_Header):
    """Durable fail-closed diagnosis for an outcome-less advanced frame branch."""

    kind: Literal["frame_relaunch_blocked"] = "frame_relaunch_blocked"
    frame_id: str
    expected_tip: str
    found_tip: str
    message: str


class FrameExited(_Header):
    kind: Literal["frame_exited"] = "frame_exited"
    frame_id: str
    attempt: int
    outcome: FrameOutcome
    text: str
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    head: str


class FrameIntegrating(_Header):
    """Durable intent that closes the ref-move -> journal crash tear."""

    kind: Literal["frame_integrating"] = "frame_integrating"
    frame_id: str
    target_frame_id: str | None = None
    integration_base: str
    candidate_head: str
    source_commits: list[CommitReceipt] = Field(default_factory=list)
    landed_commits: list[CommitReceipt] = Field(default_factory=list)


class FrameIntegrated(_Header):
    kind: Literal["frame_integrated"] = "frame_integrated"
    frame_id: str
    target_frame_id: str | None = None
    integration_base: str
    candidate_head: str
    source_commits: list[CommitReceipt] = Field(default_factory=list)
    landed_commits: list[CommitReceipt] = Field(default_factory=list)


class FramePopped(_Header):
    kind: Literal["frame_popped"] = "frame_popped"
    frame_id: str
    attempt: int
    outcome: FrameOutcome


class RunFinished(_Header):
    kind: Literal["run_finished"] = "run_finished"
    outcome: FrameOutcome
    root_head: str
    text: str


Event: TypeAlias = Annotated[
    RunOpened
    | FramePushed
    | WorktreeProvisioned
    | FrameSlotQueued
    | FrameSlotAcquired
    | FrameSlotReleased
    | DispatchCalled
    | DispatchReturned
    | GateCalled
    | GateReturned
    | Asked
    | Answered
    | CallFailed
    | WorkerReaped
    | FrameRelaunchBlocked
    | FrameExited
    | FrameIntegrating
    | FrameIntegrated
    | FramePopped
    | RunFinished,
    Field(discriminator="kind"),
]
_EVENT_ADAPTER: TypeAdapter[Event] = TypeAdapter(Event)


def parse_event(data: dict[str, object]) -> Event:
    return _EVENT_ADAPTER.validate_python(data)
