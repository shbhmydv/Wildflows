from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.test_frames import _engine
from wildflows.engine import Engine
from wildflows.events import FrameIntegrating, FramePushed
from wildflows.result import CommitReceipt
from wildflows.rig import EchoRig, RigRegistry


def test_parallel_disjoint_siblings_reapply_into_parent(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "parallel")
    assert engine.run().outcome == "ok"
    assert (repo / "left.txt").read_text(encoding="utf-8") == "0\n"
    assert (repo / "right.txt").read_text(encoding="utf-8") == "1\n"
    payload = json.loads(
        (repo / "dispatch-result.json").read_text(encoding="utf-8")
    )
    assert payload["outcome"] == "ok"
    assert [child["outcome"] for child in payload["children"]] == ["ok", "ok"]


def test_parallel_owned_paths_include_pre_move_intents(
    repo: Path, tmp_path: Path
) -> None:
    engine = Engine(
        tmp_path / "intent-run",
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id="parallel-intent",
        root_rig="echo",
        root_prompt="job",
        worktrees_root=tmp_path / "intent-worktrees",
    )
    base = engine.repository.branch_tip()
    for frame_id, parent, task_index in (
        ("f0", None, None),
        ("f0.c0.t0", "f0", 0),
    ):
        engine.journal.append(FramePushed(
            run_id="parallel-intent",
            frame_id=frame_id,
            parent_frame_id=parent,
            parent_call_index=0 if parent is not None else None,
            task_index=task_index,
            attempt=0,
            depth=0 if parent is None else 1,
            rig="echo",
            prompt="job",
            branch=engine.repository.frame_branch(frame_id),
            base_commit=base,
            worktree=str(tmp_path / frame_id),
            subtree_deadline=9999999999.0,
        ))
    receipt = CommitReceipt(sha="a" * 40, paths=["shared.txt"])
    engine.journal.append(FrameIntegrating(
        run_id="parallel-intent",
        frame_id="f0.c0.t0",
        target_frame_id="f0",
        integration_base=base,
        candidate_head="a" * 40,
        source_commits=[receipt],
        landed_commits=[receipt],
    ))
    assert engine._parallel_owned_paths("f0", 0) == {"shared.txt"}  # noqa: SLF001


def test_parallel_overlapping_siblings_refuse_later_integration(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(repo, tmp_path, monkeypatch, "conflict")
    assert engine.run().outcome == "ok"
    assert (repo / "shared.txt").read_text(encoding="utf-8") in ("0\n", "1\n")
    payload = json.loads(
        (repo / "dispatch-result.json").read_text(encoding="utf-8")
    )
    assert payload["outcome"] == "failed"
    outcomes = [child["outcome"] for child in payload["children"]]
    assert sorted(outcomes) == ["failed", "ok"]
    assert "ownership overlaps" in "\n".join(
        child["text"] for child in payload["children"]
    )
