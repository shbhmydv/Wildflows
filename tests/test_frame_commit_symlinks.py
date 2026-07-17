from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.conftest import git
from wildflows.engine import Engine
from wildflows.events import FrameCommitWarning, FrameExited, FramePushed
from wildflows.frame import FrameResult, FrameRuntime
from wildflows.rig import RigRegistry
from wildflows.workspace import IntegrationError


@dataclass
class _WritesOutOfTreeLinkRig:
    target: Path
    timeout_s: float = 10.0

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, runtime
        (workdir / "ordinary.txt").write_text("committed\n", encoding="utf-8")
        (workdir / "environment-link").symlink_to(self.target)
        return FrameResult(text="frame complete", exit_code=0)


@dataclass
class _ChangesTrackedLinkRig:
    target: Path
    timeout_s: float = 10.0

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, runtime
        link = workdir / "tracked-link"
        link.unlink()
        link.symlink_to(self.target)
        return FrameResult(text="tracked link updated", exit_code=0)


@pytest.mark.parametrize("relative_escape", [False, True], ids=["absolute", "relative"])
def test_exit_commit_skips_new_out_of_tree_symlink_and_commits_other_work(
    repo: Path, tmp_path: Path, relative_escape: bool
) -> None:
    external = tmp_path / "primary-artifact"
    external.mkdir()
    target = Path("../../primary-artifact") if relative_escape else external
    suffix = "relative" if relative_escape else "absolute"
    engine = Engine(
        tmp_path / f"run-symlink-skip-{suffix}",
        repo,
        RigRegistry({"writer": _WritesOutOfTreeLinkRig(target)}),
        run_id=f"symlink-skip-{suffix}",
        root_rig="writer",
        root_prompt="write an ordinary file and an environment link",
        worktrees_root=tmp_path / f"worktrees-symlink-skip-{suffix}",
    )

    result = engine.run()

    assert result.outcome == "ok"
    assert (repo / "ordinary.txt").read_text(encoding="utf-8") == "committed\n"
    assert not (repo / "environment-link").exists()
    root = engine.projection.frame("f0")
    assert root.head is not None
    tree_paths = git(repo, "ls-tree", "-r", "--name-only", root.head).splitlines()
    assert "ordinary.txt" in tree_paths
    assert "environment-link" not in tree_paths
    warnings = [
        event for event in engine.journal.events()
        if isinstance(event, FrameCommitWarning)
    ]
    assert len(warnings) == 1
    assert warnings[0].frame_id == "f0"
    assert warnings[0].attempt == 0
    assert warnings[0].skipped_paths == ["environment-link"]
    assert "environment-link" in warnings[0].message
    exit_event = next(
        event for event in engine.journal.events()
        if isinstance(event, FrameExited)
    )
    assert warnings[0].seq < exit_event.seq


def test_integration_refuses_directly_crafted_new_out_of_tree_symlink_commit(
    repo: Path, tmp_path: Path
) -> None:
    """Pre-fix history bypasses auto-commit, so integration needs its own guard."""
    engine = Engine(
        tmp_path / "run-crafted-link",
        repo,
        RigRegistry({"echo": _WritesOutOfTreeLinkRig(tmp_path)}),
        run_id="crafted-link",
        root_rig="echo",
        root_prompt="integration guard fixture",
        worktrees_root=tmp_path / "worktrees-crafted-link",
    )
    base = engine.repository.branch_tip()
    parent_branch = engine.repository.frame_branch("f0")
    parent = engine.repository.create_frame_worktree(
        "f0", parent_branch, base, resume=False
    )
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id="f0",
        attempt=0,
        depth=0,
        rig="echo",
        prompt="parent",
        branch=parent_branch,
        base_commit=base,
        worktree=str(parent.path),
    ))
    child_id = "f0.c0.t0"
    child_branch = engine.repository.frame_branch(child_id)
    child = engine.repository.create_frame_worktree(
        child_id, child_branch, base, resume=False
    )
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=child_id,
        parent_frame_id="f0",
        parent_call_index=0,
        task_index=0,
        attempt=0,
        depth=1,
        rig="echo",
        prompt="child",
        branch=child_branch,
        base_commit=base,
        worktree=str(child.path),
    ))
    outside = tmp_path / "outside"
    outside.mkdir()
    (child.path / "escape").symlink_to(outside)
    child_head = engine.repository.commit_all(child.path, "pre-fix unsafe link")
    engine.journal.append(FrameExited(
        run_id=engine.run_id,
        frame_id=child_id,
        attempt=0,
        outcome="ok",
        text="crafted history",
        head=child_head,
    ))
    engine.repository.remove_worktree(child)
    try:
        with pytest.raises(
            IntegrationError,
            match=r"refusing integration.*out-of-tree.*escape",
        ):
            engine._integrate_frame(  # noqa: SLF001 - integration safety boundary
                child_id,
                target_frame_id="f0",
                owned=set(),
                target_worktree=parent.path,
            )
        assert engine.repository.branch_tip(parent_branch) == base
    finally:
        engine.repository.remove_worktree(parent)


def test_already_tracked_out_of_tree_symlink_is_not_a_new_addition_warning(
    repo: Path, tmp_path: Path
) -> None:
    first_target = tmp_path / "legacy-one"
    second_target = tmp_path / "legacy-two"
    first_target.mkdir()
    second_target.mkdir()
    os.symlink(first_target, repo / "tracked-link")
    git(repo, "add", "tracked-link")
    git(repo, "commit", "-m", "track repository-owned legacy symlink")
    engine = Engine(
        tmp_path / "run-tracked-link",
        repo,
        RigRegistry({"writer": _ChangesTrackedLinkRig(second_target)}),
        run_id="tracked-link",
        root_rig="writer",
        root_prompt="update an existing tracked symlink",
        worktrees_root=tmp_path / "worktrees-tracked-link",
    )

    assert engine.run().outcome == "ok"

    assert (repo / "tracked-link").is_symlink()
    assert os.readlink(repo / "tracked-link") == str(second_target)
    assert not any(
        isinstance(event, FrameCommitWarning)
        for event in engine.journal.events()
    )
