"""Admission rails enforced at every v2 dispatch call boundary."""
from __future__ import annotations

import time
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from wildflows.frame import DispatchRequest
from wildflows.rig import RigRegistry

AdmissionCode = Literal[
    "depth_cap",
    "breadth_cap",
    "subtree_frame_cap",
    "subtree_spend_cap",
    "subtree_time_cap",
    "rig_not_allowed",
]


class AdmissionPolicy(BaseModel):
    max_depth: int = Field(default=4, ge=0)
    max_breadth: int = Field(default=8, ge=1)
    max_subtree_frames: int = Field(default=64, ge=1)
    max_subtree_spend: float = Field(default=64.0, gt=0)
    subtree_timeout_s: float = Field(default=3600.0, gt=0)
    rig_costs: dict[str, float] = Field(default_factory=dict)

    @field_validator("rig_costs")
    @classmethod
    def _positive_costs(cls, value: dict[str, float]) -> dict[str, float]:
        if any(cost <= 0 for cost in value.values()):
            raise ValueError("rig costs must be positive")
        return value

    def rig_cost(self, name: str) -> float:
        return self.rig_costs.get(name, 1.0)


class AdmissionError(RuntimeError):
    """A typed, no-effect refusal returned to the calling frame."""

    def __init__(self, code: AdmissionCode, message: str) -> None:
        super().__init__(message)
        self.code = code


def _refusal(
    code: AdmissionCode,
    message: str,
    registry: RigRegistry,
) -> AdmissionError:
    allowed = ", ".join(registry.ordered_names) or "(none)"
    return AdmissionError(code, f"{message}; allowed rigs: {allowed}")


def admit_dispatch(
    request: DispatchRequest,
    *,
    caller_depth: int,
    subtree_frames: int,
    subtree_spend: float,
    subtree_deadline: float,
    policy: AdmissionPolicy,
    registry: RigRegistry,
    now: float | None = None,
) -> None:
    """Refuse a dispatch before worktree, process, or spend effects occur."""
    observed = time.time() if now is None else now
    child_depth = caller_depth + 1
    if child_depth > policy.max_depth:
        raise _refusal(
            "depth_cap",
            f"child depth {child_depth} exceeds cap {policy.max_depth}",
            registry,
        )
    if len(request.tasks) > policy.max_breadth:
        raise _refusal(
            "breadth_cap",
            f"dispatch breadth {len(request.tasks)} exceeds cap {policy.max_breadth}",
            registry,
        )
    if subtree_frames + len(request.tasks) > policy.max_subtree_frames:
        raise _refusal(
            "subtree_frame_cap",
            f"subtree frames would exceed cap {policy.max_subtree_frames}",
            registry,
        )
    if observed >= subtree_deadline:
        raise _refusal(
            "subtree_time_cap",
            "caller subtree deadline has elapsed",
            registry,
        )
    if request.rig not in registry:
        raise _refusal(
            "rig_not_allowed",
            f"rig {request.rig!r} is not in this run's allowlist",
            registry,
        )
    projected = subtree_spend + len(request.tasks) * policy.rig_cost(request.rig)
    if projected > policy.max_subtree_spend:
        raise _refusal(
            "subtree_spend_cap",
            f"subtree spend {projected:g} would exceed cap {policy.max_subtree_spend:g}",
            registry,
        )
