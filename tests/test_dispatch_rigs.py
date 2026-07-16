from __future__ import annotations

import time
from pathlib import Path

import pytest

from wildflows.engine import Engine
from wildflows.events import CallRefused, DispatchCalled, FramePushed
from wildflows.frame import DispatchRequest, DispatchResult, ToolFailure
from wildflows.rig import EchoRig, RigRegistry
from wildflows.workspace import FrameWorktree


def _active_root(
    repo: Path, tmp_path: Path, name: str
) -> tuple[Engine, FrameWorktree]:
    registry = RigRegistry({
        "caller": EchoRig(),
        "cheap": EchoRig(),
        "senior": EchoRig(),
    })
    engine = Engine(
        tmp_path / f"run-{name}",
        repo,
        registry,
        run_id=f"dispatch-rigs-{name}",
        root_rig="caller",
        root_prompt="route tasks",
        worktrees_root=tmp_path / f"worktrees-{name}",
    )
    base = engine.repository.branch_tip()
    branch = engine.repository.frame_branch(Engine.ROOT_FRAME_ID)
    worktree = engine.repository.create_frame_worktree(
        Engine.ROOT_FRAME_ID, branch, base, resume=False
    )
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        attempt=0,
        depth=0,
        rig="caller",
        prompt="route tasks",
        branch=branch,
        base_commit=base,
        worktree=str(worktree.path),
        subtree_deadline=time.time() + 60,
    ))
    with engine._active_lock:  # noqa: SLF001 - establish a live tool caller
        engine._active[Engine.ROOT_FRAME_ID] = worktree  # noqa: SLF001
    return engine, worktree


def _close_active_root(engine: Engine, worktree: FrameWorktree) -> None:
    with engine._active_lock:  # noqa: SLF001 - counterpart to test setup
        engine._active.pop(Engine.ROOT_FRAME_ID, None)  # noqa: SLF001
    engine.repository.remove_worktree(worktree)


@pytest.mark.parametrize(
    ("selection", "expected"),
    [
        pytest.param("senior", ["senior", "senior"], id="scalar"),
        pytest.param(["cheap", "senior"], ["cheap", "senior"], id="array"),
        pytest.param(["cheap", None], ["cheap", "caller"], id="array-null-inherits"),
        pytest.param(None, ["caller", "caller"], id="omitted-inherits"),
    ],
)
def test_dispatch_rig_selection_routes_each_task_and_journals_as_given(
    repo: Path,
    tmp_path: Path,
    selection: str | list[str | None] | None,
    expected: list[str],
) -> None:
    name = "-".join(rig or "inherit" for rig in (
        [selection] if isinstance(selection, str) else selection or [None]
    ))
    engine, worktree = _active_root(repo, tmp_path, name)
    request = DispatchRequest(
        tasks=["mechanical research", "architecture judgment"],
        rig=selection,
        parallel=True,
        kinds=["review", "research"],
    )
    try:
        with engine.server:
            result = engine.handle_tool("f0", 0, "dispatch", request)
    finally:
        _close_active_root(engine, worktree)

    assert isinstance(result, DispatchResult)
    assert result.outcome == "ok", result.as_text()
    children = sorted(
        (
            event
            for event in engine.journal.events()
            if isinstance(event, FramePushed) and event.parent_frame_id == "f0"
        ),
        key=lambda event: event.task_index if event.task_index is not None else -1,
    )
    assert [child.rig for child in children] == expected
    called = next(
        event for event in engine.journal.events()
        if isinstance(event, DispatchCalled)
    )
    assert called.request.rig == selection
    assert called.request.kinds == ["review", "research"]
    digest = engine.projection.resume_digest("f0")[0]
    assert digest["request"] == request.model_dump(mode="json")
    assert digest["kinds"] == ["review", "research"]


def test_unknown_per_task_rig_is_a_durable_refusal_with_configured_names(
    repo: Path, tmp_path: Path
) -> None:
    engine, worktree = _active_root(repo, tmp_path, "unknown")
    request = DispatchRequest(
        tasks=["cheap task", "mistiered task"],
        rig=["cheap", "missing"],
        parallel=True,
    )
    try:
        with engine.server:
            result = engine.handle_tool("f0", 0, "dispatch", request)
            replayed = engine.handle_tool("f0", 0, "dispatch", request)
    finally:
        _close_active_root(engine, worktree)

    assert isinstance(result, ToolFailure)
    assert replayed == result
    assert result.error_code == "call_refused"
    assert "missing" in result.message
    assert "allowed rigs: caller, cheap, senior" in result.message
    refusals = [
        event for event in engine.journal.events()
        if isinstance(event, CallRefused)
    ]
    assert len(refusals) == 1
    assert refusals[0].request == request
    assert not any(
        isinstance(event, DispatchCalled) for event in engine.journal.events()
    )
    assert not any(
        isinstance(event, FramePushed) and event.parent_frame_id == "f0"
        for event in engine.journal.events()
    )
