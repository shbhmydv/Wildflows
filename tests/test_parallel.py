from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.test_frames import _engine


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
