"""Item-2 equivalence proof: the new RunProjection folds the pre-refactor journals to
identical decisions/state.

`tests/fixtures/journals/*.ndjson` are journals captured from the PRE-refactor engine
(seq of inplace+do+ctx, a converging loop, a capped loop, two epochs on one workdir, and
a failed-then-effectless do); each `*.snapshot.json` is the fold the OLD `ReplayState`
produced for it. This test re-folds the same ndjson with the new `RunProjection.apply`
and asserts an identical snapshot — so the projection rewrite is a pure equivalence
refactor, not a behavior change. The fixtures are FROZEN "before" truth captured once
against the old engine; they are never regenerated.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from wildflows.events import parse_event
from wildflows.projection import RunProjection

FIX = Path(__file__).resolve().parent / "fixtures" / "journals"


def _fold_fixture(ndjson: str) -> RunProjection:
    projection = RunProjection()
    for line in ndjson.splitlines():
        if line.strip():
            projection.apply(parse_event(json.loads(line)))
    return projection


def _snapshot(state: RunProjection) -> dict[str, object]:
    """Same canonical shape as tests/fixtures/gen_journal_fixtures.snapshot, over the
    new per-node projection."""
    def k(key: tuple[int, str]) -> str:
        return f"{key[0]}::{key[1]}"

    nodes = state.nodes
    return {
        "results": {k(key): n.result.model_dump()
                    for key, n in sorted(nodes.items()) if n.result is not None},
        "result_seq": {k(key): n.result_seq
                       for key, n in sorted(nodes.items()) if n.result is not None},
        "integrated": {k(key): n.integrated_paths
                       for key, n in sorted(nodes.items()) if n.integrated_paths is not None},
        "integrated_seq": {k(key): n.integrated_seq
                           for key, n in sorted(nodes.items()) if n.integrated_paths is not None},
        "dispatched": sorted(k(key) for key, n in nodes.items() if n.dispatched),
        "loop_iterations": {k(key): n.loop_iterations
                            for key, n in sorted(nodes.items()) if n.loop_iterations},
        "loop_last_commit": {k(key): n.loop_last_commit
                             for key, n in sorted(nodes.items()) if n.loop_last_iter_seq >= 0},
        "loop_last_iter_seq": {k(key): n.loop_last_iter_seq
                               for key, n in sorted(nodes.items()) if n.loop_last_iter_seq >= 0},
        "loop_converged": {k(key): n.loop_converged
                           for key, n in sorted(nodes.items()) if n.loop_last_iter_seq >= 0},
        "loop_last_body": {k(key): n.loop_last_body.model_dump()
                           for key, n in sorted(nodes.items())
                           if n.loop_last_body is not None},
        "epoch_state": {str(e): {
            "closed": state.epoch_closed(e),
            "opened": state.epoch_opened(e),
            "has_expr": state.epoch_expr(e) is not None,
        } for e in sorted(state.epochs)},
    }


def _fixture_names() -> list[str]:
    return sorted(p.stem[: -len(".snapshot")] if p.name.endswith(".snapshot.json") else p.stem
                  for p in FIX.glob("*.ndjson"))


@pytest.mark.parametrize("name", _fixture_names())
def test_new_projection_folds_old_journal_identically(name: str) -> None:
    ndjson = (FIX / f"{name}.ndjson").read_text(encoding="utf-8")
    expected = json.loads((FIX / f"{name}.snapshot.json").read_text(encoding="utf-8"))
    got = json.loads(json.dumps(_snapshot(_fold_fixture(ndjson))))  # normalize tuples/etc.
    assert got == expected
