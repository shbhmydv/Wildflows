"""One replay fold for v2 frames, calls, memoized results, and pending asks."""
from __future__ import annotations

from dataclasses import dataclass, field

from wildflows.events import (
    Answered,
    Asked,
    CallFailed,
    DispatchCalled,
    DispatchReturned,
    Event,
    FrameExited,
    FrameIntegrated,
    FrameIntegrating,
    FramePopped,
    FramePushed,
    FrameRelaunchBlocked,
    FrameSlotAcquired,
    FrameSlotQueued,
    FrameSlotReleased,
    GateCalled,
    GateReturned,
    RunFinished,
    RunOpened,
)
from wildflows.frame import (
    AskResult,
    DispatchRequest,
    FrameOutcome,
    ToolName,
    ToolRequest,
    ToolResponse,
)
@dataclass
class FrameProjection:
    frame_id: str
    parent_frame_id: str | None
    parent_call_index: int | None
    task_index: int | None
    depth: int
    rig: str
    prompt: str
    skills: list[str]
    branch: str
    base_commit: str
    worktree: str
    subtree_deadline: float
    attempt: int = 0
    push_count: int = 0
    outcome: FrameOutcome | None = None
    text: str = ""
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    head: str | None = None
    exited_seq: int = -1
    relaunch_blocked: FrameRelaunchBlocked | None = None
    integrating: FrameIntegrating | None = None
    integrated: FrameIntegrated | None = None
    popped: bool = False
    self_time_s: float = 0.0
    slot_active: bool = False
    waiting_for_slot: bool = False
    slot: int | None = None
    last_slot: int | None = None


@dataclass
class CallProjection:
    frame_id: str
    call_index: int
    call_hash: str
    tool: ToolName
    request: ToolRequest
    started_seq: int
    caller_head: str | None = None
    response: ToolResponse | None = None
    finished_seq: int = -1

    @property
    def completed(self) -> bool:
        return self.response is not None


@dataclass
class RunProjection:
    opened: RunOpened | None = None
    finished: RunFinished | None = None
    frames: dict[str, FrameProjection] = field(default_factory=dict)
    calls: dict[tuple[str, int], CallProjection] = field(default_factory=dict)
    effective_events: list[Event] = field(default_factory=list)

    def apply(self, event: Event) -> None:
        self.effective_events.append(event)
        if isinstance(event, RunOpened):
            self.opened = event
        elif isinstance(event, FramePushed):
            current = self.frames.get(event.frame_id)
            if current is None:
                current = FrameProjection(
                    frame_id=event.frame_id,
                    parent_frame_id=event.parent_frame_id,
                    parent_call_index=event.parent_call_index,
                    task_index=event.task_index,
                    depth=event.depth,
                    rig=event.rig,
                    prompt=event.prompt,
                    skills=list(event.skills),
                    branch=event.branch,
                    base_commit=event.base_commit,
                    worktree=event.worktree,
                    subtree_deadline=event.subtree_deadline,
                )
                self.frames[event.frame_id] = current
            current.attempt = event.attempt
            current.push_count += 1
            current.worktree = event.worktree
            current.popped = False
            current.relaunch_blocked = None
        elif isinstance(event, FrameSlotQueued):
            frame = self.frames[event.frame_id]
            frame.waiting_for_slot = True
        elif isinstance(event, FrameSlotAcquired):
            frame = self.frames[event.frame_id]
            frame.waiting_for_slot = False
            frame.slot_active = True
            frame.slot = event.slot
            if event.slot is not None:
                frame.last_slot = event.slot
        elif isinstance(event, FrameSlotReleased):
            frame = self.frames[event.frame_id]
            frame.self_time_s += event.active_s
            frame.slot_active = False
            frame.slot = None
            frame.waiting_for_slot = False
        elif isinstance(event, (DispatchCalled, GateCalled, Asked)):
            if isinstance(event, DispatchCalled):
                tool: ToolName = "dispatch"
                request: ToolRequest = event.request
                caller_head: str | None = event.caller_head
            elif isinstance(event, GateCalled):
                tool = "gate"
                request = event.request
                caller_head = event.caller_head
            else:
                tool = "ask"
                request = event.request
                caller_head = event.caller_head
            self.calls[(event.frame_id, event.call_index)] = CallProjection(
                frame_id=event.frame_id,
                call_index=event.call_index,
                call_hash=event.call_hash,
                tool=tool,
                request=request,
                started_seq=event.seq,
                caller_head=caller_head,
            )
        elif isinstance(event, DispatchReturned):
            call = self.calls[(event.frame_id, event.call_index)]
            call.response = event.result
            call.finished_seq = event.seq
        elif isinstance(event, GateReturned):
            call = self.calls[(event.frame_id, event.call_index)]
            call.response = event.result
            call.finished_seq = event.seq
        elif isinstance(event, Answered):
            call = self.calls[(event.frame_id, event.call_index)]
            call.response = AskResult(answer=event.answer)
            call.finished_seq = event.seq
        elif isinstance(event, CallFailed):
            key = (event.frame_id, event.call_index)
            failed_call = self.calls.get(key)
            if failed_call is None:
                failed_call = CallProjection(
                    frame_id=event.frame_id,
                    call_index=event.call_index,
                    call_hash=event.call_hash,
                    tool=event.tool,
                    request=event.request,
                    started_seq=event.seq,
                )
                self.calls[key] = failed_call
            failed_call.response = event.result
            failed_call.finished_seq = event.seq
        elif isinstance(event, FrameRelaunchBlocked):
            self.frames[event.frame_id].relaunch_blocked = event
        elif isinstance(event, FrameExited):
            frame = self.frames[event.frame_id]
            frame.outcome = event.outcome
            frame.text = event.text
            frame.exit_code = event.exit_code
            frame.stdout = event.stdout
            frame.stderr = event.stderr
            frame.head = event.head
            frame.exited_seq = event.seq
        elif isinstance(event, FrameIntegrating):
            self.frames[event.frame_id].integrating = event
        elif isinstance(event, FrameIntegrated):
            frame = self.frames[event.frame_id]
            frame.integrated = event
            frame.integrating = None
        elif isinstance(event, FramePopped):
            frame = self.frames[event.frame_id]
            frame.popped = True
            frame.outcome = event.outcome
        elif isinstance(event, RunFinished):
            self.finished = event

    def frame(self, frame_id: str) -> FrameProjection:
        try:
            return self.frames[frame_id]
        except KeyError as exc:
            raise KeyError(f"unknown frame: {frame_id}") from exc

    def call(self, frame_id: str, call_index: int) -> CallProjection | None:
        return self.calls.get((frame_id, call_index))

    def next_call_index(self, frame_id: str) -> int:
        indexes = [index for owner, index in self.calls if owner == frame_id]
        return max(indexes, default=-1) + 1

    def descendants(self, frame_id: str) -> list[FrameProjection]:
        descendants: list[FrameProjection] = []
        for candidate in self.frames.values():
            parent = candidate.parent_frame_id
            while parent is not None:
                if parent == frame_id:
                    descendants.append(candidate)
                    break
                ancestor = self.frames.get(parent)
                parent = None if ancestor is None else ancestor.parent_frame_id
        return descendants

    def pending_questions(self) -> list[CallProjection]:
        return sorted(
            (
                call
                for call in self.calls.values()
                if call.tool == "ask" and not call.completed
            ),
            key=lambda call: call.started_seq,
        )

    def resume_digest(self, frame_id: str) -> list[dict[str, object]]:
        digest: list[dict[str, object]] = []
        calls = sorted(
            (call for (owner, _), call in self.calls.items() if owner == frame_id),
            key=lambda call: call.call_index,
        )
        for call in calls:
            response = call.response
            response_data = None if response is None else response.model_dump(mode="json")
            item: dict[str, object] = {
                "call_index": call.call_index,
                "tool": call.tool,
                "content_hash": call.call_hash,
                "request": call.request.model_dump(mode="json"),
                "status": "completed" if call.completed else "pending",
                "result": response_data,
            }
            if isinstance(call.request, DispatchRequest):
                item["skills"] = [list(bundle) for bundle in call.request.skills]
                item["kinds"] = list(call.request.kinds)
            digest.append(item)
        return digest
