"""Forward-compatible decoding and explicit dashboard event support."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from pydantic import BaseModel, ValidationError

from wildflows.events import (
    SUPPORTED_JOURNAL_VERSION,
    Answered,
    Asked,
    CallFailed,
    CallRefused,
    DispatchCalled,
    DispatchReturned,
    Event,
    FrameCommitWarning,
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
    RunInterrupted,
    RunOpened,
    WorkerReaped,
    WorktreeProvisioned,
    parse_event,
)
from wildflows.projection import RunProjection


class DashboardEventDisposition(Enum):
    """How the dashboard projection handles one understood event kind."""

    PROJECT = "project"
    NO_OP = "no_op"


DASHBOARD_EVENT_DISPOSITIONS: Mapping[
    type[BaseModel], DashboardEventDisposition
] = MappingProxyType({
    RunOpened: DashboardEventDisposition.PROJECT,
    FramePushed: DashboardEventDisposition.PROJECT,
    WorktreeProvisioned: DashboardEventDisposition.NO_OP,
    FrameSlotQueued: DashboardEventDisposition.PROJECT,
    FrameSlotAcquired: DashboardEventDisposition.PROJECT,
    FrameSlotReleased: DashboardEventDisposition.PROJECT,
    DispatchCalled: DashboardEventDisposition.PROJECT,
    DispatchReturned: DashboardEventDisposition.PROJECT,
    GateCalled: DashboardEventDisposition.PROJECT,
    GateReturned: DashboardEventDisposition.PROJECT,
    Asked: DashboardEventDisposition.PROJECT,
    Answered: DashboardEventDisposition.PROJECT,
    CallRefused: DashboardEventDisposition.PROJECT,
    CallFailed: DashboardEventDisposition.PROJECT,
    WorkerReaped: DashboardEventDisposition.NO_OP,
    FrameRelaunchBlocked: DashboardEventDisposition.PROJECT,
    FrameCommitWarning: DashboardEventDisposition.NO_OP,
    FrameExited: DashboardEventDisposition.PROJECT,
    FrameIntegrating: DashboardEventDisposition.PROJECT,
    FrameIntegrated: DashboardEventDisposition.PROJECT,
    FramePopped: DashboardEventDisposition.PROJECT,
    RunInterrupted: DashboardEventDisposition.PROJECT,
    RunFinished: DashboardEventDisposition.PROJECT,
})
# Review-visible registration of every event the dashboard understands.


def dashboard_projection_ready(projection: RunProjection, event: Event) -> bool:
    """Whether state required by a dependent event survived tolerant replay."""
    if isinstance(event, (DispatchReturned, GateReturned, Answered)):
        return (event.frame_id, event.call_index) in projection.calls
    if isinstance(event, (
        FrameSlotQueued,
        FrameSlotAcquired,
        FrameSlotReleased,
        FrameRelaunchBlocked,
        FrameExited,
        FrameIntegrating,
        FrameIntegrated,
        FramePopped,
    )):
        return event.frame_id in projection.frames
    return True


@dataclass(frozen=True)
class DashboardJournalRecord:
    """One structurally readable record and its optional understood event."""

    position: int
    raw: dict[str, object]
    kind: str
    event: Event | None
    disposition: DashboardEventDisposition | None
    newer_version: int | None = None


def _not_understood(
    *,
    position: int,
    data: dict[str, object],
    kind: str,
    newer_version: int | None = None,
) -> DashboardJournalRecord:
    return DashboardJournalRecord(
        position=position,
        raw=data,
        kind=kind,
        event=None,
        disposition=None,
        newer_version=newer_version,
    )


def decode_dashboard_record(
    data: dict[str, object], position: int
) -> DashboardJournalRecord:
    """Decode one physical journal record without rejecting newer vocabulary."""
    kind = data.get("kind")
    if not isinstance(kind, str) or not kind:
        raise ValueError(f"journal record {position} has no event kind")

    seq = data.get("seq")
    if type(seq) is int and seq != position:
        raise ValueError(
            f"journal seq {seq} is not contiguous (expected {position})"
        )
    if type(seq) is not int:
        return _not_understood(
            position=position,
            data=data,
            kind=kind,
        )

    version = data.get("version")
    if type(version) is not int:
        return _not_understood(
            position=position,
            data=data,
            kind=kind,
        )

    normalized = data
    newer_version: int | None = None
    if version > SUPPORTED_JOURNAL_VERSION:
        newer_version = version
        normalized = dict(data)
        normalized["version"] = SUPPORTED_JOURNAL_VERSION

    try:
        event = parse_event(normalized)
    except ValidationError:
        return _not_understood(
            position=position,
            data=data,
            kind=kind,
            newer_version=newer_version,
        )

    disposition = DASHBOARD_EVENT_DISPOSITIONS.get(type(event))
    if disposition is None:
        return _not_understood(
            position=position,
            data=data,
            kind=kind,
            newer_version=newer_version,
        )
    return DashboardJournalRecord(
        position=position,
        raw=data,
        kind=kind,
        event=event,
        disposition=disposition,
        newer_version=newer_version,
    )
