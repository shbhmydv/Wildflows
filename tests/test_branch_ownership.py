from __future__ import annotations

import time
from pathlib import Path

import pytest

from tests.conftest import git
from wildflows.engine import Engine
from wildflows.events import FramePushed
from wildflows.frame import FrameResult, FrameRuntime
from wildflows.rig import RigRegistry
from wildflows.workspace import FrameOwnershipError


class NeverRunRig:
    """Makes execution of an unowned frame observable and fatal."""

    timeout_s = 30.0

    def __init__(self, marker: Path) -> None:
        self.marker = marker

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, workdir, runtime
        self.marker.write_text("executed\n", encoding="utf-8")
        raise AssertionError("a fresh frame must not adopt an existing branch")


def test_fresh_frame_rejects_preexisting_branch_without_adopting_it(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "rig-executed"
    worktrees_root = tmp_path / "frame-worktrees"
    engine = Engine(
        tmp_path / "run",
        repo,
        RigRegistry({"never": NeverRunRig(marker)}),
        run_id="fresh-frame-ownership",
        root_rig="never",
        root_prompt="must not execute",
        worktrees_root=worktrees_root,
    )
    branch = engine.repository.frame_branch(Engine.ROOT_FRAME_ID)
    base = engine.repository.branch_tip()
    git(repo, "update-ref", branch, base)
    assert engine.repository.ref_exists(branch)
    assert Engine.ROOT_FRAME_ID not in engine.projection.frames
    worktree_creation_attempted = False

    def reject_worktree_creation(*args: object, **kwargs: object) -> None:
        del args, kwargs
        nonlocal worktree_creation_attempted
        worktree_creation_attempted = True
        pytest.fail("a fresh frame must reject the branch before creating a worktree")

    monkeypatch.setattr(
        engine.repository, "create_frame_worktree", reject_worktree_creation
    )

    with pytest.raises(FrameOwnershipError):
        engine._launch_frame(  # noqa: SLF001 - direct frame-launch ownership seam
            frame_id=Engine.ROOT_FRAME_ID,
            parent_frame_id=None,
            parent_call_index=None,
            task_index=None,
            depth=0,
            rig="never",
            prompt="must not execute",
            skills=[],
            base_commit=base,
            subtree_deadline=time.time() + 30,
        )

    assert not marker.exists()
    assert engine.repository.branch_tip(branch) == base
    assert not worktree_creation_attempted
    assert Engine.ROOT_FRAME_ID not in engine.projection.frames
    assert not any(
        isinstance(event, FramePushed) for event in engine.journal.events()
    )
