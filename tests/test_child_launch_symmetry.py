from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest

from wildflows.engine import Engine
from wildflows.events import DispatchCalled, DispatchReturned, FramePushed
from wildflows.frame import DispatchRequest, DispatchResult
from wildflows.journal import JournalPoisonedError
from wildflows.rig import EchoRig, RigRegistry
from wildflows.workspace import FrameWorktree


def _engine_with_active_root(repo: Path, tmp_path: Path, name: str) -> tuple[Engine, FrameWorktree]:
    engine = Engine(
        tmp_path / f"run-{name}",
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id=f"child-launch-{name}",
        root_rig="echo",
        root_prompt="root job",
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
        rig="echo",
        prompt="root job",
        skills=[],
        branch=branch,
        base_commit=base,
        worktree=str(worktree.path),
    ))
    with engine._active_lock:  # noqa: SLF001 - establish a live tool caller
        engine._active[Engine.ROOT_FRAME_ID] = worktree  # noqa: SLF001
    return engine, worktree


def _remove_active_root(engine: Engine, worktree: FrameWorktree) -> None:
    with engine._active_lock:  # noqa: SLF001 - counterpart to test setup
        engine._active.pop(Engine.ROOT_FRAME_ID, None)  # noqa: SLF001
    engine.repository.remove_worktree(worktree)


@pytest.mark.parametrize(
    ("parallel", "tasks"),
    [
        pytest.param(False, ["only child"], id="serial"),
        pytest.param(True, ["left child", "right child"], id="parallel"),
    ],
)
def test_pre_push_child_launch_fault_is_durable_and_memoized_in_both_modes(
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    parallel: bool,
    tasks: list[str],
) -> None:
    engine, parent_worktree = _engine_with_active_root(
        repo, tmp_path, "parallel" if parallel else "serial"
    )
    request = DispatchRequest(tasks=tasks, rig="echo", parallel=parallel)
    launch_attempts: list[str] = []
    attempt_lock = threading.Lock()

    def fail_before_frame_pushed(**kwargs: object) -> object:
        frame_id = kwargs["frame_id"]
        assert isinstance(frame_id, str)
        with attempt_lock:
            launch_attempts.append(frame_id)
        raise RuntimeError("injected ordinary pre-push child launch fault")

    monkeypatch.setattr(engine, "_launch_frame", fail_before_frame_pushed)
    try:
        first = engine.handle_tool(Engine.ROOT_FRAME_ID, 0, "dispatch", request)
        assert isinstance(first, DispatchResult)

        expected_ids = [f"f0.c0.t{index}" for index in range(len(tasks))]
        assert first.outcome == "failed"
        assert [child.frame_id for child in first.children] == expected_ids
        assert all(child.outcome == "failed" for child in first.children)
        assert all(
            "injected ordinary pre-push child launch fault" in child.text
            for child in first.children
        )
        assert sorted(launch_attempts) == expected_ids

        events = engine.journal.events()
        returned = [event for event in events if isinstance(event, DispatchReturned)]
        assert len([event for event in events if isinstance(event, DispatchCalled)]) == 1
        assert len(returned) == 1
        assert returned[0].result == first
        assert not any(
            isinstance(event, FramePushed)
            and event.parent_frame_id == Engine.ROOT_FRAME_ID
            for event in events
        )

        replayed = engine.handle_tool(Engine.ROOT_FRAME_ID, 0, "dispatch", request)
        assert isinstance(replayed, DispatchResult)
        assert replayed == first
        assert sorted(launch_attempts) == expected_ids
        assert len([
            event for event in engine.journal.events()
            if isinstance(event, DispatchReturned)
        ]) == 1
    finally:
        _remove_active_root(engine, parent_worktree)


def test_parallel_journal_poisoning_remains_fail_closed_not_a_child_result(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, parent_worktree = _engine_with_active_root(repo, tmp_path, "poison")
    request = DispatchRequest(
        tasks=["left child", "right child"], rig="echo", parallel=True
    )
    original_append = engine.journal.append

    def poison_on_child_push(event: object) -> int:
        if (
            isinstance(event, FramePushed)
            and event.parent_frame_id == Engine.ROOT_FRAME_ID
        ):
            def fail_fsync(_: int) -> None:
                raise OSError("injected journal fsync failure")

            with monkeypatch.context() as patch:
                patch.setattr(os, "fsync", fail_fsync)
                return original_append(event)
        return original_append(event)  # type: ignore[arg-type]

    monkeypatch.setattr(engine.journal, "append", poison_on_child_push)
    try:
        with pytest.raises((OSError, JournalPoisonedError)):
            engine.handle_tool(Engine.ROOT_FRAME_ID, 0, "dispatch", request)

        events = engine.journal.events()
        assert len([event for event in events if isinstance(event, DispatchCalled)]) == 1
        assert not any(isinstance(event, DispatchReturned) for event in events)
    finally:
        _remove_active_root(engine, parent_worktree)
