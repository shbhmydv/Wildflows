from __future__ import annotations

from pathlib import Path

import pytest

from tests.conftest import git
from wildflows.engine import Engine
from wildflows.events import FrameExited, FrameIntegrated, FramePopped, RunFinished
from wildflows.frame import FrameResult, FrameRuntime
from wildflows.rig import RigRegistry
from wildflows.workspace import IntegrationError


class RootEffectRig:
    """A deterministic successful root whose effect must be committed first."""

    timeout_s = 30.0

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt
        assert runtime.frame_id == Engine.ROOT_FRAME_ID
        (workdir / "root-effect.txt").write_text("root effect\n", encoding="utf-8")
        return FrameResult(outcome="ok", text="root effect complete", exit_code=0)


def _tracked_files(worktree: Path) -> dict[str, bytes]:
    paths = (part for part in git(worktree, "ls-files", "-z").split("\0") if part)
    return {path: (worktree / path).read_bytes() for path in paths}


def test_root_unwind_refuses_a_run_branch_owned_by_another_worktree(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_branch = "run-branch"
    linked_owner = tmp_path / "linked-run-branch-owner"

    # Make the configured repository a detached worktree, then give the configured
    # run branch to a distinct linked worktree.  The engine must not treat that
    # linked worktree as authority to advance this run's root branch.
    git(repo, "checkout", "-b", run_branch)
    git(repo, "checkout", "--detach")
    git(repo, "worktree", "add", str(linked_owner), run_branch)
    assert git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
    assert git(linked_owner, "symbolic-ref", "--quiet", "HEAD") == (
        f"refs/heads/{run_branch}"
    )

    run_tip_before = git(repo, "rev-parse", f"refs/heads/{run_branch}")
    linked_head_before = git(linked_owner, "rev-parse", "HEAD")
    linked_files_before = _tracked_files(linked_owner)
    linked_status_before = git(
        linked_owner, "status", "--porcelain=v1", "--untracked-files=all"
    )
    assert linked_status_before == ""

    engine = Engine(
        tmp_path / "run",
        repo,
        RigRegistry({"root-effect": RootEffectRig()}),
        run_id="root-integration-owner",
        root_rig="root-effect",
        root_prompt="write the root effect",
        run_branch=run_branch,
        worktrees_root=tmp_path / "frame-worktrees",
    )
    refusals: list[IntegrationError] = []
    integrate = engine._integrate_frame  # noqa: SLF001 - observe the root unwind seam

    def record_refusal(
        frame_id: str,
        *,
        target_frame_id: str | None,
        owned: set[str],
        target_worktree: Path | None = None,
    ) -> FrameIntegrated:
        try:
            return integrate(
                frame_id,
                target_frame_id=target_frame_id,
                owned=owned,
                target_worktree=target_worktree,
            )
        except IntegrationError as exc:
            refusals.append(exc)
            raise

    monkeypatch.setattr(engine, "_integrate_frame", record_refusal)
    result = engine.run()

    # The root's successful effect was committed, but its integration was refused by
    # the typed integration boundary rather than being merged in the linked owner.
    assert len(refusals) == 1
    assert result.outcome == "failed"
    assert result.text == f"root integration failed: {refusals[0]}"
    root = engine.projection.frame(Engine.ROOT_FRAME_ID)
    assert root.outcome == "failed"
    assert root.integrated is None

    events = engine.journal.events()
    root_exits = [
        event
        for event in events
        if isinstance(event, FrameExited) and event.frame_id == Engine.ROOT_FRAME_ID
    ]
    assert len(root_exits) == 1
    assert root_exits[0].outcome == "ok"
    assert root_exits[0].head != run_tip_before
    assert git(repo, "show", f"{root_exits[0].head}:root-effect.txt") == "root effect"
    assert [
        (event.frame_id, event.outcome)
        for event in events
        if isinstance(event, FramePopped)
    ] == [(Engine.ROOT_FRAME_ID, "failed")]
    finished = [event for event in events if isinstance(event, RunFinished)]
    assert len(finished) == 1
    assert finished[0].outcome == "failed"
    assert finished[0].text == result.text

    # A refusal must leave both the branch ref and every observable aspect of the
    # unrelated linked worktree exactly as they were before the root unwind.
    assert git(repo, "rev-parse", f"refs/heads/{run_branch}") == run_tip_before
    assert git(linked_owner, "rev-parse", "HEAD") == linked_head_before
    assert _tracked_files(linked_owner) == linked_files_before
    assert git(
        linked_owner, "status", "--porcelain=v1", "--untracked-files=all"
    ) == linked_status_before
