from __future__ import annotations

from wildflows.events import DispatchCalled, parse_event
import pytest
from pydantic import ValidationError

from wildflows.frame import DispatchRequest, call_hash
from wildflows.projection import RunProjection
from wildflows.rig import EchoRig, RigRegistry


def _dispatch_event(request: dict[str, object]) -> dict[str, object]:
    return {
        "version": 2,
        "seq": 0,
        "ts": 1.0,
        "run_id": "run",
        "kind": "dispatch_called",
        "frame_id": "f0",
        "call_index": 0,
        "call_hash": "digest",
        "request": request,
        "caller_head": "a" * 40,
    }


def test_dispatch_kinds_are_per_task_free_text_and_replayed() -> None:
    event = parse_event(_dispatch_event({
        "tasks": ["build", "inspect"],
        "rig": "worker",
        "parallel": False,
        "skills": [[], []],
        "kinds": ["implement", "domain-specific critique"],
    }))
    assert isinstance(event, DispatchCalled)
    assert event.request.kinds == ["implement", "domain-specific critique"]

    projection = RunProjection()
    projection.apply(event)
    assert projection.call("f0", 0) is not None
    assert projection.resume_digest("f0")[0]["kinds"] == [
        "implement", "domain-specific critique",
    ]


def test_dispatch_kinds_validate_parallel_length_and_affect_identity() -> None:
    plain = DispatchRequest(tasks=["build"], rig="worker")
    implement = DispatchRequest(
        tasks=["build"], rig="worker", kinds=["implement"]
    )
    assert call_hash("dispatch", plain) != call_hash("dispatch", implement)
    with pytest.raises(ValidationError, match="one string per task"):
        DispatchRequest(
            tasks=["build", "inspect"], rig="worker", kinds=["implement"]
        )
    with pytest.raises(ValidationError, match="must be non-blank"):
        DispatchRequest(tasks=["build"], rig="worker", kinds=["  "])


def test_old_dispatch_event_without_kinds_still_loads() -> None:
    event = parse_event(_dispatch_event({
        "tasks": ["legacy"],
        "rig": "worker",
        "parallel": False,
        "skills": [[]],
    }))
    assert isinstance(event, DispatchCalled)
    assert event.request.kinds == []


def test_kind_mapping_supplies_per_task_rigs_and_explicit_rig_wins() -> None:
    registry = RigRegistry(
        {"worker": EchoRig(), "senior": EchoRig()},
        kinds={"implement": "worker", "research": "senior"},
    )
    mapped = DispatchRequest(
        tasks=["build", "study"], kinds=["implement", "research"]
    )
    assert registry.task_rigs(mapped.rig, mapped.kinds, len(mapped.tasks)) == (
        "worker", "senior",
    )
    explicit = DispatchRequest(
        tasks=["build", "study"],
        rig="senior",
        kinds=["implement", "research"],
    )
    assert registry.task_rigs(explicit.rig, explicit.kinds, len(explicit.tasks)) == (
        "senior", "senior",
    )
