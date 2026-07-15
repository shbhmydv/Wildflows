from __future__ import annotations

from pathlib import Path

import pytest

from wildflows.engine import Engine, FrameRelaunchBlockedError
from wildflows.events import (
    FrameExited,
    FrameIntegrated,
    FrameIntegrating,
    FramePushed,
    FrameRelaunchBlocked,
)
from wildflows.frame import FrameResult, FrameRuntime
from wildflows.journal import Journal
from wildflows.rig import RigRegistry
from wildflows.workspace import FrameWorktree, Repository


_FUTURE_DEADLINE = 4_102_444_800.0


class ObservableRig:
    """Records any relaunch that reaches a rig."""

    timeout_s = 30.0

    def __init__(self) -> None:
        self.frame_ids: list[str] = []

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt
        self.frame_ids.append(runtime.frame_id)
        (workdir / "rerun.txt").write_text("rerun\n", encoding="utf-8")
        return FrameResult(text="rerun completed", exit_code=0)


def _push(
    engine: Engine,
    *,
    frame_id: str,
    branch: str,
    base_commit: str,
    worktree: Path,
    parent_frame_id: str | None = None,
    parent_call_index: int | None = None,
    task_index: int | None = None,
    depth: int = 0,
    prompt: str = "root job",
) -> None:
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=frame_id,
        parent_frame_id=parent_frame_id,
        parent_call_index=parent_call_index,
        task_index=task_index,
        attempt=0,
        depth=depth,
        rig="observable",
        prompt=prompt,
        skills=[],
        branch=branch,
        base_commit=base_commit,
        worktree=str(worktree),
        subtree_deadline=_FUTURE_DEADLINE,
    ))


@pytest.mark.parametrize("seam", ["root", "child"])
def test_resume_blocks_unexplained_frame_branch_advance_before_relaunch(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, seam: str
) -> None:
    """A commit after push but before exit is not safe to replay as a new attempt."""
    run_dir = tmp_path / f"{seam}-blocked-run"
    worktrees = tmp_path / f"{seam}-blocked-worktrees"
    rig = ObservableRig()
    initial = Engine(
        run_dir,
        repo,
        RigRegistry({"observable": rig}),
        run_id=f"{seam}-blocked",
        root_rig="observable",
        root_prompt="root job",
        worktrees_root=worktrees,
    )
    base = initial.repository.branch_tip()

    if seam == "root":
        frame_id = Engine.ROOT_FRAME_ID
        branch = initial.repository.frame_branch(frame_id)
        parent_frame_id = None
        parent_call_index = None
        task_index = None
        depth = 0
        prompt = "root job"
    else:
        root_branch = initial.repository.frame_branch(Engine.ROOT_FRAME_ID)
        initial.repository.git(["update-ref", root_branch, base])
        _push(
            initial,
            frame_id=Engine.ROOT_FRAME_ID,
            branch=root_branch,
            base_commit=base,
            worktree=worktrees / "lost-root-worktree",
        )
        frame_id = "f0.c0.t0"
        branch = initial.repository.frame_branch(frame_id)
        parent_frame_id = Engine.ROOT_FRAME_ID
        parent_call_index = 0
        task_index = 0
        depth = 1
        prompt = "child job"

    # This is the durable state immediately before the H4 crash window: frame_pushed
    # exists and the attempt worktree's branch is committed, but frame_exited does not.
    crashed_worktree = initial.repository.create_frame_worktree(
        frame_id, branch, base, resume=False
    )
    _push(
        initial,
        frame_id=frame_id,
        branch=branch,
        base_commit=base,
        worktree=crashed_worktree.path,
        parent_frame_id=parent_frame_id,
        parent_call_index=parent_call_index,
        task_index=task_index,
        depth=depth,
        prompt=prompt,
    )
    (crashed_worktree.path / "committed-before-frame-exited.txt").write_text(
        "must not be replayed\n", encoding="utf-8"
    )
    found = initial.repository.commit_all(
        crashed_worktree.path, "core commit before frame_exited crash"
    )
    assert found != base
    assert initial.repository.branch_tip(branch) == found

    created: list[str] = []
    removed: list[Path] = []

    def forbid_resume_worktree(
        self: Repository,
        requested_frame_id: str,
        requested_branch: str,
        requested_base: str,
        *,
        resume: bool,
    ) -> FrameWorktree:
        del self, requested_base
        created.append(f"{requested_frame_id}:{requested_branch}:{resume}")
        raise AssertionError("blocked relaunch must not create or touch a resume worktree")

    def forbid_resume_removal(
        self: Repository, worktree: FrameWorktree
    ) -> None:
        del self
        removed.append(worktree.path)
        raise AssertionError("blocked relaunch must not remove a resume worktree")

    monkeypatch.setattr(Repository, "create_frame_worktree", forbid_resume_worktree)
    monkeypatch.setattr(Repository, "remove_worktree", forbid_resume_removal)

    with pytest.raises(FrameRelaunchBlockedError) as raised:
        resumed = Engine(
            run_dir,
            repo,
            RigRegistry({"observable": rig}),
            run_id=f"{seam}-blocked",
            root_rig="observable",
            root_prompt="root job",
        )
        if seam == "root":
            resumed.run()
        else:
            resumed._execute_child(  # noqa: SLF001 - child relaunch resume seam
                resumed.projection.frame(Engine.ROOT_FRAME_ID),
                0,
                0,
                "child job",
                "observable",
                [],
                base_commit=base,
            )

    operator_message = str(raised.value)
    assert frame_id in operator_message
    assert base in operator_message
    assert found in operator_message
    assert "operator" in operator_message.lower()
    assert rig.frame_ids == []
    assert created == []
    assert removed == []
    assert crashed_worktree.path.is_dir()
    assert (crashed_worktree.path / "committed-before-frame-exited.txt").read_text(
        encoding="utf-8"
    ) == "must not be replayed\n"

    # The block itself must survive a process boundary; an in-memory exception is not
    # enough to tell a later operator why this frame cannot be resumed.
    reloaded = Journal.load(run_dir)
    blocks = [
        event for event in reloaded.events() if isinstance(event, FrameRelaunchBlocked)
    ]
    assert len(blocks) == 1
    assert blocks[0].frame_id == frame_id
    assert blocks[0].expected_tip == base
    assert blocks[0].found_tip == found
    assert "operator" in blocks[0].message.lower()
    assert len([
        event
        for event in reloaded.events()
        if isinstance(event, FramePushed) and event.frame_id == frame_id
    ]) == 1


def test_resume_relaunches_outcome_less_frame_at_unadvanced_tip(
    repo: Path, tmp_path: Path
) -> None:
    """The ordinary push-without-exit replay path remains unchanged."""
    run_dir = tmp_path / "unadvanced-tip-run"
    worktrees = tmp_path / "unadvanced-tip-worktrees"
    rig = ObservableRig()
    initial = Engine(
        run_dir,
        repo,
        RigRegistry({"observable": rig}),
        run_id="unadvanced-tip",
        root_rig="observable",
        root_prompt="root job",
        worktrees_root=worktrees,
    )
    base = initial.repository.branch_tip()
    branch = initial.repository.frame_branch(Engine.ROOT_FRAME_ID)
    initial.repository.git(["update-ref", branch, base])
    _push(
        initial,
        frame_id=Engine.ROOT_FRAME_ID,
        branch=branch,
        base_commit=base,
        worktree=worktrees / "lost-root-worktree",
    )

    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"observable": rig}),
        run_id="unadvanced-tip",
        root_rig="observable",
        root_prompt="root job",
    )
    assert resumed.run().outcome == "ok"
    assert rig.frame_ids == [Engine.ROOT_FRAME_ID]
    pushes = [
        event
        for event in resumed.journal.events()
        if isinstance(event, FramePushed) and event.frame_id == Engine.ROOT_FRAME_ID
    ]
    assert [event.attempt for event in pushes] == [0, 1]
    assert not any(
        isinstance(event, FrameRelaunchBlocked)
        for event in resumed.journal.events()
    )


def test_resume_relaunches_outcome_less_frame_at_journal_explained_tip(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unexited parent may legitimately be advanced by its integrated child."""
    run_dir = tmp_path / "explained-tip-run"
    worktrees = tmp_path / "explained-tip-worktrees"
    rig = ObservableRig()
    initial = Engine(
        run_dir,
        repo,
        RigRegistry({"observable": rig}),
        run_id="explained-tip",
        root_rig="observable",
        root_prompt="root job",
        worktrees_root=worktrees,
    )
    base = initial.repository.branch_tip()
    root_branch = initial.repository.frame_branch(Engine.ROOT_FRAME_ID)
    initial.repository.git(["update-ref", root_branch, base])
    _push(
        initial,
        frame_id=Engine.ROOT_FRAME_ID,
        branch=root_branch,
        base_commit=base,
        worktree=worktrees / "interrupted-root-worktree",
    )

    # A completed child advances the still-live root branch.  That advance is explained
    # by frame_integrating/frame_integrated and is therefore the root's resume tip, not
    # an H4 divergence from its original base.
    child_id = "f0.c0.t0"
    child_branch = initial.repository.frame_branch(child_id)
    child_worktree = initial.repository.create_frame_worktree(
        child_id, child_branch, base, resume=False
    )
    _push(
        initial,
        frame_id=child_id,
        branch=child_branch,
        base_commit=base,
        worktree=child_worktree.path,
        parent_frame_id=Engine.ROOT_FRAME_ID,
        parent_call_index=0,
        task_index=0,
        depth=1,
        prompt="already completed child",
    )
    (child_worktree.path / "child-before-root-relaunch.txt").write_text(
        "child\n", encoding="utf-8"
    )
    child_head = initial.repository.commit_all(child_worktree.path, "child completed")
    initial.journal.append(FrameExited(
        run_id=initial.run_id,
        frame_id=child_id,
        attempt=0,
        outcome="ok",
        text="child completed",
        exit_code=0,
        head=child_head,
    ))
    receipt = initial.repository.receipt(base, child_head)
    integrating = FrameIntegrating(
        run_id=initial.run_id,
        frame_id=child_id,
        target_frame_id=Engine.ROOT_FRAME_ID,
        integration_base=base,
        candidate_head=child_head,
        source_commits=receipt.commits,
        landed_commits=receipt.commits,
    )
    initial.journal.append(integrating)
    initial.repository.advance(
        root_branch, base, child_head, target_worktree=None
    )
    initial.journal.append(FrameIntegrated(
        run_id=initial.run_id,
        frame_id=child_id,
        target_frame_id=Engine.ROOT_FRAME_ID,
        integration_base=base,
        candidate_head=child_head,
        source_commits=receipt.commits,
        landed_commits=receipt.commits,
    ))
    initial.repository.remove_worktree(child_worktree)
    assert initial.repository.branch_tip(root_branch) == child_head

    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"observable": rig}),
        run_id="explained-tip",
        root_rig="observable",
        root_prompt="root job",
    )
    real_create = resumed.repository.create_frame_worktree
    real_remove = resumed.repository.remove_worktree
    created: list[FrameWorktree] = []
    removed: list[FrameWorktree] = []

    def observe_create(
        frame_id: str, branch: str, base_commit: str, *, resume: bool
    ) -> FrameWorktree:
        assert frame_id == Engine.ROOT_FRAME_ID
        assert branch == root_branch
        assert base_commit == base
        assert resume is True
        worktree = real_create(frame_id, branch, base_commit, resume=resume)
        created.append(worktree)
        return worktree

    def observe_remove(worktree: FrameWorktree) -> None:
        removed.append(worktree)
        real_remove(worktree)

    monkeypatch.setattr(resumed.repository, "create_frame_worktree", observe_create)
    monkeypatch.setattr(resumed.repository, "remove_worktree", observe_remove)

    assert resumed.run().outcome == "ok"
    assert rig.frame_ids == [Engine.ROOT_FRAME_ID]
    assert (repo / "child-before-root-relaunch.txt").read_text(encoding="utf-8") == (
        "child\n"
    )
    assert (repo / "rerun.txt").read_text(encoding="utf-8") == "rerun\n"
    assert len(created) == len(removed) == 1
    assert removed == created

    events = resumed.journal.events()
    root_pushes = [
        event
        for event in events
        if isinstance(event, FramePushed) and event.frame_id == Engine.ROOT_FRAME_ID
    ]
    assert [event.attempt for event in root_pushes] == [0, 1]
    assert len([
        event
        for event in events
        if isinstance(event, FrameExited) and event.frame_id == Engine.ROOT_FRAME_ID
    ]) == 1
    assert not any(isinstance(event, FrameRelaunchBlocked) for event in events)
