from __future__ import annotations

from pathlib import Path

import pytest

from wildflows.admission import AdmissionError, AdmissionPolicy, admit_dispatch
from wildflows.engine import Engine
from wildflows.events import DispatchCalled, FramePushed
from wildflows.frame import DispatchRequest, call_hash
from wildflows.rig import EchoRig, RigRegistry


@pytest.mark.parametrize(
    ("dispatch", "kwargs", "code", "message"),
    [
        (
            DispatchRequest(tasks=["x"], rig="ok"),
            {"caller_depth": 1},
            "depth_cap",
            "child depth 2 exceeds max_depth=1 (--max-depth)",
        ),
        (
            DispatchRequest(tasks=["x", "y"], rig="ok"),
            {},
            "breadth_cap",
            "dispatch breadth 2 exceeds max_breadth=1 (--max-breadth)",
        ),
        (
            DispatchRequest(tasks=["x"], rig="ok"),
            {"subtree_frames": 1},
            "subtree_frame_cap",
            (
                "subtree frames 2 would exceed max_subtree_frames=1 "
                "(--max-subtree-frames)"
            ),
        ),
        (
            DispatchRequest(tasks=["x"], rig="ok"),
            {"subtree_spend": 0.5},
            "subtree_spend_cap",
            (
                "subtree spend 1.5 would exceed max_subtree_spend=1 "
                "(--max-subtree-spend)"
            ),
        ),
        (
            DispatchRequest(tasks=["x"], rig="missing"),
            {},
            "rig_not_allowed",
            (
                "selected rig 'missing' is not in rig_allowlist=[ok] (rigs.yaml)"
            ),
        ),
        (
            DispatchRequest(tasks=["x"]),
            {},
            "rig_not_allowed",
            (
                "selected rig None cannot resolve against rig_allowlist=[ok] "
                "(rigs.yaml): dispatch without rig requires the caller's rig"
            ),
        ),
    ],
)
def test_dispatch_rails_refuse_before_effects(
    dispatch: DispatchRequest,
    kwargs: dict[str, int | float],
    code: str,
    message: str,
) -> None:
    values: dict[str, int | float] = {
        "caller_depth": 0,
        "subtree_frames": 0,
        "subtree_spend": 0.0,
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
            policy=policy,
            registry=RigRegistry({"ok": EchoRig()}),
        )
    assert raised.value.code == code
    assert str(raised.value) == f"{message}; allowed rigs: ok"


def test_unknown_rig_refusal_lists_registry_keys_in_operator_order() -> None:
    registry = RigRegistry({
        "senior": EchoRig(),
        "senior-terra": EchoRig(),
        "local": EchoRig(),
    })

    with pytest.raises(AdmissionError) as raised:
        admit_dispatch(
            DispatchRequest(tasks=["bounded work"], rig="worker-local"),
            caller_depth=0,
            subtree_frames=0,
            subtree_spend=0.0,
            policy=AdmissionPolicy(),
            registry=registry,
        )

    assert raised.value.code == "rig_not_allowed"
    assert str(raised.value) == (
        "selected rig 'worker-local' is not in "
        "rig_allowlist=[senior, senior-terra, local] (rigs.yaml); "
        "allowed rigs: senior, senior-terra, local"
    )


def test_per_task_rigs_and_inherited_entries_resolve_before_spend_admission() -> None:
    registry = RigRegistry({"worker": EchoRig(), "senior": EchoRig()})
    request = DispatchRequest(
        tasks=["build", "study"],
        rig=["worker", None],
        kinds=["review", "research"],
    )
    resolved = admit_dispatch(
        request,
        caller_depth=0,
        caller_rig="senior",
        subtree_frames=0,
        subtree_spend=0.0,
        policy=AdmissionPolicy(
            max_breadth=2,
            max_subtree_spend=4,
            rig_costs={"worker": 1, "senior": 3},
        ),
        registry=registry,
    )
    assert resolved == ("worker", "senior")

    inherited = admit_dispatch(
        DispatchRequest(tasks=["critique"], kinds=["review"]),
        caller_depth=0,
        caller_rig="senior",
        subtree_frames=0,
        subtree_spend=0.0,
        policy=AdmissionPolicy(max_subtree_spend=4),
        registry=registry,
    )
    assert inherited == ("senior",)


def test_dispatch_is_admitted_regardless_of_durable_run_age(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    monkeypatch.setattr("wildflows.engine.time.time", lambda: clock[0])
    engine = Engine(
        tmp_path / "old-run",
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id="old-run",
        root_rig="echo",
        root_prompt="job",
        worktrees_root=tmp_path / "old-worktrees",
    )
    opened = engine.projection.opened
    assert opened is not None
    assert opened.started_at == 100.0
    base = engine.repository.branch_tip()
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        attempt=0,
        depth=0,
        rig="echo",
        prompt="job",
        branch=engine.repository.frame_branch(Engine.ROOT_FRAME_ID),
        base_commit=base,
        worktree=str(tmp_path / "old-root"),
    ))

    clock[0] += 12 * 60 * 60
    frame = engine.projection.frame(Engine.ROOT_FRAME_ID)
    engine._admit_and_reserve(  # noqa: SLF001 - admission age regression
        frame,
        0,
        DispatchRequest(tasks=["healthy long-run child"], rig="echo"),
    )
    assert (frame.frame_id, 0) in engine._dispatch_reservations  # noqa: SLF001


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
