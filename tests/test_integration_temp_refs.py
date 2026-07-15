from __future__ import annotations

from pathlib import Path

import pytest

from wildflows.engine import Engine
from wildflows.events import FrameExited, FrameIntegrated, FrameIntegrating, FramePushed
from wildflows.rig import EchoRig, RigRegistry
from wildflows.workspace import FrameWorktree


def _engine(repo: Path, tmp_path: Path, name: str) -> Engine:
    return Engine(
        tmp_path / f"{name}-run",
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id=name,
        root_rig="echo",
        root_prompt="temporary integration ref regression",
        worktrees_root=tmp_path / f"{name}-worktrees",
    )


def _pushed(
    engine: Engine,
    worktree: FrameWorktree,
    *,
    parent_frame_id: str | None,
    task_index: int | None,
    depth: int,
    prompt: str,
) -> None:
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=worktree.frame_id,
        parent_frame_id=parent_frame_id,
        parent_call_index=0 if parent_frame_id is not None else None,
        task_index=task_index,
        attempt=0,
        depth=depth,
        rig="echo",
        prompt=prompt,
        skills=[],
        branch=worktree.branch,
        base_commit=worktree.base_commit,
        worktree=str(worktree.path),
        subtree_deadline=9_999_999_999.0,
    ))


def _exited_child(
    engine: Engine,
    *,
    frame_id: str,
    base: str,
    task_index: int,
    filename: str,
) -> tuple[FrameWorktree, str]:
    branch = engine.repository.frame_branch(frame_id)
    worktree = engine.repository.create_frame_worktree(
        frame_id, branch, base, resume=False
    )
    _pushed(
        engine,
        worktree,
        parent_frame_id=Engine.ROOT_FRAME_ID,
        task_index=task_index,
        depth=1,
        prompt=frame_id,
    )
    (worktree.path / filename).write_text(f"{frame_id}\n", encoding="utf-8")
    head = engine.repository.commit_all(worktree.path, f"commit {frame_id}")
    engine.journal.append(FrameExited(
        run_id=engine.run_id,
        frame_id=frame_id,
        attempt=0,
        outcome="ok",
        text="child complete",
        exit_code=0,
        head=head,
    ))
    engine.repository.remove_worktree(worktree)
    return worktree, head


def test_reapplied_candidate_is_held_by_temp_ref_until_frame_integrated(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A later parallel sibling must not be unreferenced between reapply and move."""
    engine = _engine(repo, tmp_path, "reapply-temp-ref")
    base = engine.repository.branch_tip()

    parent_branch = engine.repository.frame_branch(Engine.ROOT_FRAME_ID)
    parent = engine.repository.create_frame_worktree(
        Engine.ROOT_FRAME_ID, parent_branch, base, resume=False
    )
    try:
        _pushed(
            engine,
            parent,
            parent_frame_id=None,
            task_index=None,
            depth=0,
            prompt="parent",
        )
        # This is the already-integrated earlier sibling.  The later sibling below
        # therefore cannot fast-forward its original frame head into the parent.
        (parent.path / "earlier-sibling.txt").write_text("earlier\n", encoding="utf-8")
        moving_base = engine.repository.commit_all(parent.path, "earlier sibling")

        child_id = "f0.c0.t1"
        _, source_head = _exited_child(
            engine,
            frame_id=child_id,
            base=base,
            task_index=1,
            filename="later-sibling.txt",
        )
        temporary_ref = engine.repository.integration_ref(child_id)
        original_advance = engine.repository.advance
        candidate_at_advance: str | None = None

        def observe_pre_advance(
            target_ref: str,
            integration_base: str,
            candidate: str,
            *,
            target_worktree: Path | None,
        ) -> None:
            nonlocal candidate_at_advance
            candidate_at_advance = candidate
            # reapply's detached integrator has already gone away at this boundary.
            assert not list(engine.repository.worktrees_root.glob("integrate-*"))
            assert target_ref == parent_branch
            assert integration_base == moving_base
            assert candidate != source_head
            assert engine.repository.branch_tip(target_ref) == moving_base
            # Exact tip equality proves reachability through the engine-owned ref,
            # rather than incidental object retention or reflogs.
            assert engine.repository.ref_exists(temporary_ref)
            assert engine.repository.branch_tip(temporary_ref) == candidate
            original_advance(
                target_ref,
                integration_base,
                candidate,
                target_worktree=target_worktree,
            )

        original_append = engine.journal.append
        integrated_while_pinned = False

        def observe_durable_integration(event: object) -> None:
            nonlocal integrated_while_pinned
            original_append(event)  # type: ignore[arg-type]
            if isinstance(event, FrameIntegrated) and event.frame_id == child_id:
                # This executes after the append/fsync and projection fold, but before
                # _integrate_frame can clean up its temporary ref.
                assert engine.repository.ref_exists(temporary_ref)
                assert engine.repository.branch_tip(temporary_ref) == event.candidate_head
                assert engine.projection.frame(child_id).integrated is not None
                integrated_while_pinned = True

        monkeypatch.setattr(engine.repository, "advance", observe_pre_advance)
        monkeypatch.setattr(engine.journal, "append", observe_durable_integration)

        integrated = engine._integrate_frame(  # noqa: SLF001 - integration boundary
            child_id,
            target_frame_id=Engine.ROOT_FRAME_ID,
            owned=set(),
            target_worktree=parent.path,
        )

        assert candidate_at_advance == integrated.candidate_head
        assert integrated_while_pinned
        assert engine.repository.branch_tip(parent_branch) == integrated.candidate_head
        assert not engine.repository.ref_exists(temporary_ref)
    finally:
        engine.repository.remove_worktree(parent)


def test_constructor_sweeps_unjournaled_temp_ref_but_keeps_durable_refs(
    repo: Path, tmp_path: Path
) -> None:
    """Only a temp ref with no durable frame_integrating owner is an orphan."""
    first = _engine(repo, tmp_path, "constructor-temp-ref-sweep")
    base = first.repository.branch_tip()

    parent_branch = first.repository.frame_branch(Engine.ROOT_FRAME_ID)
    parent = first.repository.create_frame_worktree(
        Engine.ROOT_FRAME_ID, parent_branch, base, resume=False
    )
    _pushed(
        first,
        parent,
        parent_frame_id=None,
        task_index=None,
        depth=0,
        prompt="parent",
    )
    (parent.path / "earlier-sibling.txt").write_text("earlier\n", encoding="utf-8")
    moving_base = first.repository.commit_all(parent.path, "earlier sibling")
    first.repository.remove_worktree(parent)

    valid_id = "f0.c0.t0"
    _, valid_source_head = _exited_child(
        first,
        frame_id=valid_id,
        base=base,
        task_index=0,
        filename="valid-source.txt",
    )
    valid_source = first.repository.receipt(base, valid_source_head)
    valid_candidate, valid_landed = first.repository.reapply(
        valid_source.commits, moving_base
    )
    valid_temp_ref = first.repository.integration_ref(valid_id)
    first.repository.git(["update-ref", valid_temp_ref, valid_candidate])
    first.journal.append(FrameIntegrating(
        run_id=first.run_id,
        frame_id=valid_id,
        target_frame_id=Engine.ROOT_FRAME_ID,
        integration_base=moving_base,
        candidate_head=valid_candidate,
        source_commits=valid_source.commits,
        landed_commits=valid_landed.commits,
    ))

    orphan_id = "f0.c0.t1"
    _, orphan_source_head = _exited_child(
        first,
        frame_id=orphan_id,
        base=base,
        task_index=1,
        filename="orphan-source.txt",
    )
    orphan_source = first.repository.receipt(base, orphan_source_head)
    orphan_candidate, _ = first.repository.reapply(
        orphan_source.commits, moving_base
    )
    orphan_temp_ref = first.repository.integration_ref(orphan_id)
    first.repository.git(["update-ref", orphan_temp_ref, orphan_candidate])
    assert first.repository.ref_exists(valid_temp_ref)
    assert first.repository.ref_exists(orphan_temp_ref)

    # An ordinary durable branch in the same repository must never be confused with
    # a crash orphan merely because its commit is also reachable from a temp ref.
    preserved_ref = "refs/heads/keep-durable-ref"
    first.repository.git(["update-ref", preserved_ref, valid_source_head])

    resumed = Engine(
        first.run_dir,
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id=first.run_id,
        root_rig="echo",
        root_prompt="temporary integration ref regression",
    )

    assert not resumed.repository.ref_exists(orphan_temp_ref)
    assert resumed.repository.ref_exists(valid_temp_ref)
    assert resumed.repository.branch_tip(valid_temp_ref) == valid_candidate
    assert resumed.repository.ref_exists(first.repository.frame_branch(valid_id))
    assert resumed.repository.branch_tip(preserved_ref) == valid_source_head
