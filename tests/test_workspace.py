from __future__ import annotations

from pathlib import Path

from wildflows.workspace import Repository


def test_requested_in_repo_worktree_root_is_forced_outside(
    repo: Path, tmp_path: Path
) -> None:
    repository = Repository(
        repo,
        tmp_path / "run",
        "outside",
        worktrees_root=repo / "nested-worktrees",
    )
    assert not repository.worktrees_root.is_relative_to(repo)
    assert repository.worktrees_root.parent == repo.parent / ".target-wildflows-worktrees"
