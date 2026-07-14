from __future__ import annotations

import time
from pathlib import Path

import pytest

from wildflows.admission import AdmissionError, AdmissionPolicy, admit_dispatch
from wildflows.engine import Engine
from wildflows.events import DispatchCalled, FramePushed
from wildflows.frame import DispatchRequest, call_hash
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


def test_pending_dispatch_reservation_is_rebuilt_on_restart(
    repo: Path, tmp_path: Path
) -> None:
    run_dir = tmp_path / "run"
    policy = AdmissionPolicy(
        max_depth=4,
        max_breadth=2,
        max_subtree_frames=2,
        max_subtree_spend=10,
    )
    registry = RigRegistry({"echo": EchoRig()})
    first = Engine(
        run_dir,
        repo,
        registry,
        run_id="reserved",
        root_rig="echo",
        root_prompt="job",
        policy=policy,
        worktrees_root=tmp_path / "worktrees",
    )
    base = first.repository.branch_tip()
    root_branch = first.repository.frame_branch("f0")
    first.journal.append(FramePushed(
        run_id="reserved",
        frame_id="f0",
        attempt=0,
        depth=0,
        rig="echo",
        prompt="job",
        branch=root_branch,
        base_commit=base,
        worktree=str(tmp_path / "old-root"),
        subtree_deadline=time.time() + 60,
    ))
    request = DispatchRequest(tasks=["one", "two"], rig="echo")
    first.journal.append(DispatchCalled(
        run_id="reserved",
        frame_id="f0",
        call_index=0,
        call_hash=call_hash("dispatch", request),
        request=request,
        caller_head=base,
    ))
    first.journal.append(FramePushed(
        run_id="reserved",
        frame_id="f0.c0.t0",
        parent_frame_id="f0",
        parent_call_index=0,
        task_index=0,
        attempt=0,
        depth=1,
        rig="echo",
        prompt="one",
        branch=first.repository.frame_branch("f0.c0.t0"),
        base_commit=base,
        worktree=str(tmp_path / "old-child"),
        subtree_deadline=time.time() + 60,
    ))

    resumed = Engine(
        run_dir,
        repo,
        registry,
        run_id="reserved",
        root_rig="echo",
        root_prompt="job",
    )
    child = resumed.projection.frame("f0.c0.t0")
    with pytest.raises(AdmissionError) as raised:
        resumed._admit_and_reserve(  # noqa: SLF001 - durability boundary regression
            child,
            0,
            DispatchRequest(tasks=["grandchild"], rig="echo"),
        )
    assert raised.value.code == "subtree_frame_cap"
