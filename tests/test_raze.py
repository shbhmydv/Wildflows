from __future__ import annotations

import importlib.util
from pathlib import Path


def test_v1_execution_surface_is_absent() -> None:
    assert importlib.util.find_spec("wildflows.expr") is None
    assert importlib.util.find_spec("wildflows.planner") is None
    assert not Path("wildflows/macros").exists()
    assert not Path("rigs/planner-picodex.sh").exists()
    assert not Path("docs/PLANNER-RIG.md").exists()

    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in Path("wildflows").rglob("*.py")
    )
    for dead_name in (
        "PlannerDecision",
        "admit_epoch",
        "run_epoch",
        "parse_expr",
        "NodeProjection",
        "ExecutionOutcome",
    ):
        assert dead_name not in source
