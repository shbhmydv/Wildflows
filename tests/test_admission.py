"""Whole-tree admission rules retained across the execution-core rebuild."""
from __future__ import annotations

import pytest

from wildflows.admission import AdmissionError, admit_epoch
from wildflows.events import Boundary
from wildflows.expr import CtxRef, Dispatch, Do, Edit, Expr, Inplace, RigRef, Seq
from wildflows.projection import RunProjection
from wildflows.rig import EchoRig, RigRegistry


def admit(tree: Expr) -> Expr:
    return admit_epoch(tree, 0, RunProjection(), RigRegistry({"echo": EchoRig()}))


def test_admission_dealiases_reused_python_nodes() -> None:
    shared = Do(task="same object", rig=RigRef(name="echo"))
    tree = admit(Seq(children=[shared, shared]))
    assert isinstance(tree, Seq)
    assert [child.node_id for child in tree.children] == ["n0.0", "n0.1"]
    assert tree.children[0] is not tree.children[1]
    assert shared.node_id == ""


def test_unknown_rig_is_rejected() -> None:
    with pytest.raises(AdmissionError, match="unknown rig"):
        admit(Do(task="x", rig=RigRef(name="missing")))


def test_ctx_must_reference_upstream_resultful_seq_sibling() -> None:
    valid = Seq(
        children=[
            Inplace(edits=[Edit(path="x", content="x")]),
            Do(task="use", rig=RigRef(name="echo"), ctx=[CtxRef(kind="node", ref="n0.0")]),
        ]
    )
    admit(valid)

    forward = Seq(
        children=[
            Do(task="bad", rig=RigRef(name="echo"), ctx=[CtxRef(kind="node", ref="n0.1")]),
            Inplace(edits=[]),
        ]
    )
    with pytest.raises(AdmissionError, match="not upstream"):
        admit(forward)


def test_ctx_cannot_cross_dispatch_siblings() -> None:
    tree = Dispatch(
        children=[
            Inplace(edits=[]),
            Do(task="bad", rig=RigRef(name="echo"), ctx=[CtxRef(kind="node", ref="n0.0")]),
        ]
    )
    with pytest.raises(AdmissionError, match="crosses a Dispatch"):
        admit(tree)


def test_open_epoch_rejects_changed_resume_tree() -> None:
    projection = RunProjection()
    registry = RigRegistry({"echo": EchoRig()})
    original = admit_epoch(Do(task="original", rig=RigRef(name="echo")), 0, projection, registry)
    projection.apply(Boundary(
        seq=0, run_id="run", epoch=0, node_id="n0", phase="opened",
        expr=original.model_dump(),
    ))
    with pytest.raises(AdmissionError, match="differs"):
        admit_epoch(Do(task="changed", rig=RigRef(name="echo")), 0, projection, registry)


def test_composite_last_child_must_produce_result() -> None:
    with pytest.raises(AdmissionError, match="no result-producing last leaf"):
        admit(Seq(children=[Do(task="ok", rig=RigRef(name="echo")), Seq(children=[])]))
