from __future__ import annotations

import time

import pytest

from wildflows.admission import AdmissionError, AdmissionPolicy, admit_dispatch
from wildflows.frame import DispatchRequest
from wildflows.rig import EchoRig, RigRegistry


@pytest.mark.parametrize(
    ("dispatch", "kwargs", "code"),
    [
        (
            DispatchRequest(tasks=["x"], rig="ok"),
            {"caller_depth": 1},
            "depth_cap",
        ),
        (
            DispatchRequest(tasks=["x", "y"], rig="ok"),
            {},
            "breadth_cap",
        ),
        (
            DispatchRequest(tasks=["x"], rig="ok"),
            {"subtree_frames": 1},
            "subtree_frame_cap",
        ),
        (
            DispatchRequest(tasks=["x"], rig="ok"),
            {"subtree_spend": 0.5},
            "subtree_spend_cap",
        ),
        (
            DispatchRequest(tasks=["x"], rig="missing"),
            {},
            "rig_not_allowed",
        ),
        (
            DispatchRequest(tasks=["x"], rig="ok"),
            {"subtree_deadline": 0.0},
            "subtree_time_cap",
        ),
    ],
)
def test_dispatch_rails_refuse_before_effects(
    dispatch: DispatchRequest, kwargs: dict[str, int | float], code: str
) -> None:
    values: dict[str, int | float] = {
        "caller_depth": 0,
        "subtree_frames": 0,
        "subtree_spend": 0.0,
        "subtree_deadline": time.time() + 60,
    }
    values.update(kwargs)
    policy = AdmissionPolicy(
        max_depth=1,
        max_breadth=1,
        max_subtree_frames=1,
        max_subtree_spend=1.0,
    )
    with pytest.raises(AdmissionError) as raised:
        admit_dispatch(
            dispatch,
            caller_depth=int(values["caller_depth"]),
            subtree_frames=int(values["subtree_frames"]),
            subtree_spend=float(values["subtree_spend"]),
            subtree_deadline=float(values["subtree_deadline"]),
            policy=policy,
            registry=RigRegistry({"ok": EchoRig()}),
        )
    assert raised.value.code == code
